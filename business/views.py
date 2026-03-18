from functools import lru_cache
import math

import requests
from django.contrib.gis.geos import Point
from django.contrib.gis.measure import D
from django.db.models import Count, OuterRef, Subquery
from django.http import JsonResponse
from django.shortcuts import render

from .models import Batiment, Enterprise, LegalUnitPeriod


NAF_SECTION_RANGES = [
	(1, 3, "Section A - Agriculture, forestry and fishing"),
	(5, 9, "Section B - Mining and quarrying"),
	(10, 33, "Section C - Manufacturing"),
	(35, 35, "Section D - Electricity, gas, steam and air conditioning"),
	(36, 39, "Section E - Water supply, sewerage, waste management"),
	(41, 43, "Section F - Construction"),
	(45, 47, "Section G - Wholesale and retail trade; repair"),
	(49, 53, "Section H - Transportation and storage"),
	(55, 56, "Section I - Accommodation and food service"),
	(58, 63, "Section J - Information and communication"),
	(64, 66, "Section K - Financial and insurance activities"),
	(68, 68, "Section L - Real estate activities"),
	(69, 75, "Section M - Professional, scientific and technical"),
	(77, 82, "Section N - Administrative and support services"),
	(84, 84, "Section O - Public administration and defence"),
	(85, 85, "Section P - Education"),
	(86, 88, "Section Q - Human health and social work"),
	(90, 93, "Section R - Arts, entertainment and recreation"),
	(94, 96, "Section S - Other service activities"),
	(97, 98, "Section T - Households as employers"),
	(99, 99, "Section U - Activities of extraterritorial organizations"),
]


@lru_cache(maxsize=256)
def geocode_address(query: str):
	if not query:
		return None
	response = requests.get(
		"https://nominatim.openstreetmap.org/search",
		params={"q": query, "format": "json", "limit": 1},
		headers={"User-Agent": "sirene-dashboard/1.0"},
		timeout=8,
	)
	response.raise_for_status()
	payload = response.json()
	if not payload:
		return None
	first = payload[0]
	return {
		"lat": float(first["lat"]),
		"lon": float(first["lon"]),
		"display_name": first.get("display_name", query),
	}


def haversine_km(lat1, lon1, lat2, lon2):
	earth_radius_km = 6371.0
	phi1 = math.radians(lat1)
	phi2 = math.radians(lat2)
	dphi = math.radians(lat2 - lat1)
	dlambda = math.radians(lon2 - lon1)
	a = (
		math.sin(dphi / 2) ** 2
		+ math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
	)
	return 2 * earth_radius_km * math.atan2(math.sqrt(a), math.sqrt(1 - a))


@lru_cache(maxsize=4096)
def naf_label(code: str):
	if not code:
		return "N/A"
	prefix = "".join(char for char in str(code) if char.isdigit())
	if len(prefix) < 2:
		return f"Unknown section ({code})"
	try:
		section_num = int(prefix[:2])
	except ValueError:
		return f"Unknown section ({code})"
	for start, end, label in NAF_SECTION_RANGES:
		if start <= section_num <= end:
			return f"{code} — {label}"
	return f"{code} — Unknown section"


def dashboard_view(request):
	top_activity = (
		LegalUnitPeriod.objects.exclude(activite_principale__isnull=True)
		.exclude(activite_principale="")
		.values("activite_principale")
		.annotate(total=Count("id"))
		.order_by("-total")
		.first()
	)

	context = {
		"total_enterprises": Enterprise.objects.count(),
		"total_batiments": Batiment.objects.count(),
		"located_batiments": Batiment.objects.exclude(location__isnull=True).count(),
		"active_legal_units": LegalUnitPeriod.objects.filter(
			date_fin__isnull=True,
			etat_administratif="A",
		)
		.values("legal_unit_id")
		.distinct()
		.count(),
		"top_activity": top_activity["activite_principale"] if top_activity else "N/A",
	}
	return render(request, "enterprises/dashboard.html", context)


def dashboard_naf_codes(request):
	try:
		limit = int(request.GET.get("limit", 5000))
	except (TypeError, ValueError):
		limit = 5000
	limit = max(100, min(limit, 50000))

	rows = (
		LegalUnitPeriod.objects.exclude(activite_principale__isnull=True)
		.exclude(activite_principale="")
		.values("activite_principale")
		.annotate(count=Count("id"))
		.order_by("activite_principale")[:limit]
	)

	payload = [
		{
			"code": row["activite_principale"],
			"label": naf_label(row["activite_principale"]),
			"count": row["count"],
		}
		for row in rows
	]
	return JsonResponse({"codes": payload, "meta": {"count": len(payload)}})


def dashboard_map_data(request):
	activity = request.GET.get("activity", "").strip()
	geo_query = request.GET.get("geo", "").strip()
	cursor = request.GET.get("cursor", "").strip()
	try:
		radius_km = float(request.GET.get("radius", 25))
	except (TypeError, ValueError):
		radius_km = 25.0

	try:
		page_size = int(request.GET.get("page_size", 250))
	except (TypeError, ValueError):
		page_size = 250
	page_size = max(50, min(page_size, 1000))

	latest_period = LegalUnitPeriod.objects.filter(
		legal_unit_id=OuterRef("enterprise__siren")
	).order_by("-date_debut", "-id")

	queryset = (
		Batiment.objects.exclude(location__isnull=True)
		.select_related("enterprise", "enterprise__legal_unit")
		.annotate(
			latest_activity=Subquery(latest_period.values("activite_principale")[:1]),
			latest_denomination=Subquery(latest_period.values("denomination")[:1]),
			latest_nom=Subquery(latest_period.values("nom")[:1]),
			latest_admin_state=Subquery(latest_period.values("etat_administratif")[:1]),
		)
		.order_by("siret")
	)

	if activity:
		queryset = queryset.filter(
			enterprise__legal_unit__periods__activite_principale__icontains=activity
		).distinct()

	geocoded_center = None
	geocode_error = None
	center_point = None
	if geo_query:
		try:
			geocoded_center = geocode_address(geo_query)
		except requests.RequestException:
			geocode_error = "Geocoding service is temporarily unavailable."

		if geocoded_center:
			center_point = Point(geocoded_center["lon"], geocoded_center["lat"], srid=4326)
			if radius_km > 0:
				queryset = queryset.filter(location__distance_lte=(center_point, D(km=radius_km)))
		else:
			queryset = queryset.filter(postal_code__icontains=geo_query)

	if cursor:
		queryset = queryset.filter(siret__gt=cursor)

	markers = []
	has_more = False
	next_cursor = None
	
	for batiment in queryset.iterator(chunk_size=500):
		if not batiment.location:
			continue

		lon = float(batiment.location.x)
		lat = float(batiment.location.y)

		distance_km = None
		if geocoded_center and radius_km > 0:
			distance_km = haversine_km(
				geocoded_center["lat"],
				geocoded_center["lon"],
				lat,
				lon,
			)

		denomination = batiment.latest_denomination or batiment.latest_nom or "Unknown"
		markers.append(
			{
				"siren": batiment.enterprise_id,
				"siret": batiment.siret,
				"lat": lat,
				"lon": lon,
				"postal_code": batiment.postal_code,
				"activity": batiment.latest_activity,
				"state": batiment.latest_admin_state,
				"name": denomination,
				"distance_km": round(distance_km, 2) if distance_km is not None else None,
			}
		)

		if len(markers) > page_size:
			markers.pop()
			has_more = True
			next_cursor = markers[-1]["siret"]
			break

	return JsonResponse(
		{
			"center": geocoded_center,
			"radius_km": radius_km,
			"markers": markers,
			"pagination": {
				"cursor": next_cursor,
				"has_more": has_more,
				"page_size": page_size,
			},
			"meta": {
				"count": len(markers),
				"geocode_error": geocode_error,
			},
		}
	)
