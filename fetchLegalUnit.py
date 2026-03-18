import matplotlib.pyplot as plt
import django
import time
import os
import sys
import threading
from dataclasses import dataclass, field

import requests

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "sirene.settings")
django.setup()

from django.utils import timezone
from django.utils.dateparse import parse_date, parse_datetime

from business.models import Business, LegalUnit, LegalUnitPeriod

PROGRESS_FIG = None
PROGRESS_AX = None
PROGRESS_BARS = None
PROGRESS_TEXT = None

API_URL = "https://api.insee.fr/api-sirene/3.11/siren/"
API_KEY = "e937b663-a15b-4cb2-b7b6-63a15b7cb289"


@dataclass
class ProcessingState:
    total_records: int = 0
    processed_records: int = 0
    process_start_time: float | None = None
    current_batch_speed: float = 0.0
    average_speed: float = 0.0
    old_average_speed: float = 0.0
    is_done: bool = False
    batch_size: int = 1000
    optimize_direction: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    next_cursor: str = "*"


STATE = ProcessingState()


def clean_value(value):
    if value is None or value == "" or value == "[ND]":
        return None
    if isinstance(value, str):
        return value
    return str(value)


def make_aware_datetime(dt):
    if dt is None:
        return None
    if timezone.is_naive(dt):
        return timezone.make_aware(dt, timezone.get_current_timezone())
    return dt


def make_request():
    with STATE.lock:
        batch_size = STATE.batch_size
        current_cursor = STATE.next_cursor

    res = requests.get(
        API_URL,
        params={"nombre": batch_size, "curseur": current_cursor},
        headers={"X-INSEE-Api-Key-Integration": API_KEY},
        timeout=60,
    )
    if res.status_code in {400, 409, 429}:
        print("Rate limit reached, waiting for 10 seconds...", current_cursor, res.status_code)
        time.sleep(10)
        return make_request()

    res.raise_for_status()
    data = res.json()
    if "header" in data: 
        with STATE.lock:
            STATE.next_cursor = data["header"].get("curseurSuivant", "")
    else:        
        print("No header in response, cannot update cursor. Stopping.")
        return None
    batch = data.get("unitesLegales", [])
    if not batch:
        print("No more data to fetch.")
        return None
    return batch


def process_request_in_batch(processor=None):
    while True:
        batch = make_request()
        if not batch:
            break
        if processor:
            processor(batch)
        else:
            print(f"Batch processed: {len(batch)} rows")


def process_batch(batch):
    batch_start = time.perf_counter()

    with STATE.lock:
        if STATE.process_start_time is None:
            STATE.process_start_time = batch_start

    siren_list = [clean_value(unit.get("siren")) for unit in batch if clean_value(unit.get("siren"))]
    existing_businesses = set(
        Business.objects.filter(siren__in=siren_list).values_list("siren", flat=True)
    )
    existing_legal_units_by_siren = {
        legal_unit.siren: legal_unit
        for legal_unit in LegalUnit.objects.filter(siren__in=siren_list)
    }

    existing_period_keys = set(
        LegalUnitPeriod.objects.filter(legal_unit_id__in=siren_list).values_list(
            "legal_unit_id",
            "date_debut",
            "date_fin",
            "activite_principale",
            "etat_administratif",
            "nom",
            "denomination",
        )
    )

    legal_units_to_create = []
    periods_to_create = []

    new_legal_units_by_siren = {}
    legal_units_to_update_by_siren = {}
    seen_period_keys = set()

    legal_unit_update_fields = [
        "business",
        "statut_diffusion",
        "unite_purgee",
        "date_creation",
        "sexe",
        "prenom1",
        "prenom2",
        "prenom3",
        "prenom4",
        "prenom_usuel",
        "pseudonyme",
        "identifiant_association",
        "tranche_effectifs",
        "annee_effectifs",
        "date_dernier_traitement",
        "nombre_periodes",
        "categorie_entreprise",
        "annee_categorie_entreprise",
        "activite_principale_naf25",
    ]

    for unit in batch:
        siren = clean_value(unit.get("siren"))
        if not siren or siren not in existing_businesses:
            continue

        date_creation_str = clean_value(unit.get("dateCreationUniteLegale"))
        date_dernier_traitement_str = clean_value(unit.get("dateDernierTraitementUniteLegale"))

        legal_unit_data = {
            "business_id": siren,
            "siren": siren,
            "statut_diffusion": clean_value(unit.get("statutDiffusionUniteLegale")),
            "unite_purgee": unit.get("unitePurgeeUniteLegale"),
            "date_creation": parse_date(date_creation_str) if date_creation_str else None,
            "sexe": clean_value(unit.get("sexeUniteLegale")),
            "prenom1": clean_value(unit.get("prenom1UniteLegale")),
            "prenom2": clean_value(unit.get("prenom2UniteLegale")),
            "prenom3": clean_value(unit.get("prenom3UniteLegale")),
            "prenom4": clean_value(unit.get("prenom4UniteLegale")),
            "prenom_usuel": clean_value(unit.get("prenomUsuelUniteLegale")),
            "pseudonyme": clean_value(unit.get("pseudonymeUniteLegale")),
            "identifiant_association": clean_value(unit.get("identifiantAssociationUniteLegale")),
            "tranche_effectifs": clean_value(unit.get("trancheEffectifsUniteLegale")),
            "annee_effectifs": clean_value(unit.get("anneeEffectifsUniteLegale")),
            "date_dernier_traitement": make_aware_datetime(parse_datetime(date_dernier_traitement_str))
            if date_dernier_traitement_str
            else None,
            "nombre_periodes": unit.get("nombrePeriodesUniteLegale", 0),
            "categorie_entreprise": clean_value(unit.get("categorieEntreprise")),
            "annee_categorie_entreprise": clean_value(unit.get("anneeCategorieEntreprise")),
            "activite_principale_naf25": clean_value(unit.get("activitePrincipaleNAF25UniteLegale")),
        }

        existing_legal_unit = existing_legal_units_by_siren.get(siren)
        if existing_legal_unit is not None:
            existing_legal_unit.business_id = siren
            for key, value in legal_unit_data.items():
                if key not in {"business_id", "siren"}:
                    setattr(existing_legal_unit, key, value)
            legal_units_to_update_by_siren[siren] = existing_legal_unit
        else:
            existing_new_legal_unit = new_legal_units_by_siren.get(siren)
            if existing_new_legal_unit is None:
                new_legal_unit = LegalUnit(**legal_unit_data)
                new_legal_units_by_siren[siren] = new_legal_unit
                legal_units_to_create.append(new_legal_unit)
            else:
                for key, value in legal_unit_data.items():
                    if key not in {"business_id", "siren"}:
                        setattr(existing_new_legal_unit, key, value)
        periods = unit.get("periodesUniteLegale", [])
        for period_data in periods:
            date_debut_str = clean_value(period_data.get("dateDebut"))
            date_fin_str = clean_value(period_data.get("dateFin"))

            date_debut = parse_date(date_debut_str) if date_debut_str else None
            date_fin = parse_date(date_fin_str) if date_fin_str else None
            activite_principale = clean_value(period_data.get("activitePrincipaleUniteLegale"))
            etat_administratif = clean_value(period_data.get("etatAdministratifUniteLegale"))
            nom = clean_value(period_data.get("nomUniteLegale"))
            denomination = clean_value(period_data.get("denominationUniteLegale"))

            period_key = (
                siren,
                date_debut,
                date_fin,
                activite_principale,
                etat_administratif,
                nom,
                denomination,
            )

            if period_key in existing_period_keys or period_key in seen_period_keys:
                continue

            seen_period_keys.add(period_key)
            periods_to_create.append(
                LegalUnitPeriod(
                    legal_unit_id=siren,
                    date_debut=date_debut,
                    date_fin=date_fin,
                    etat_administratif=etat_administratif,
                    changement_etat_administratif=period_data.get(
                        "changementEtatAdministratifUniteLegale", False
                    ),
                    nom=nom,
                    changement_nom=period_data.get("changementNomUniteLegale", False),
                    nom_usage=clean_value(period_data.get("nomUsageUniteLegale")),
                    changement_nom_usage=period_data.get("changementNomUsageUniteLegale", False),
                    denomination=denomination,
                    changement_denomination=period_data.get("changementDenominationUniteLegale", False),
                    denomination_usuelle1=clean_value(period_data.get("denominationUsuelle1UniteLegale")),
                    denomination_usuelle2=clean_value(period_data.get("denominationUsuelle2UniteLegale")),
                    denomination_usuelle3=clean_value(period_data.get("denominationUsuelle3UniteLegale")),
                    changement_denomination_usuelle=period_data.get(
                        "changementDenominationUsuelleUniteLegale", False
                    ),
                    categorie_juridique=clean_value(period_data.get("categorieJuridiqueUniteLegale")),
                    changement_categorie_juridique=period_data.get(
                        "changementCategorieJuridiqueUniteLegale", False
                    ),
                    activite_principale=activite_principale,
                    nomenclature_activite_principale=clean_value(
                        period_data.get("nomenclatureActivitePrincipaleUniteLegale")
                    ),
                    changement_activite_principale=period_data.get(
                        "changementActivitePrincipaleUniteLegale", False
                    ),
                    nic_siege=clean_value(period_data.get("nicSiegeUniteLegale")),
                    changement_nic_siege=period_data.get("changementNicSiegeUniteLegale", False),
                    economie_sociale_solidaire=clean_value(
                        period_data.get("economieSocialeSolidaireUniteLegale")
                    ),
                    changement_economie_sociale_solidaire=period_data.get(
                        "changementEconomieSocialeSolidaireUniteLegale", False
                    ),
                    societe_mission=clean_value(period_data.get("societeMissionUniteLegale")),
                    changement_societe_mission=period_data.get("changementSocieteMissionUniteLegale", False),
                    caractere_employeur=clean_value(period_data.get("caractereEmployeurUniteLegale")),
                    changement_caractere_employeur=period_data.get(
                        "changementCaractereEmployeurUniteLegale", False
                    ),
                )
            )

    legal_units_to_update = list(legal_units_to_update_by_siren.values())

    if legal_units_to_create:
        LegalUnit.objects.bulk_create(legal_units_to_create, ignore_conflicts=True)
    if legal_units_to_update:
        LegalUnit.objects.bulk_update(legal_units_to_update, fields=legal_unit_update_fields)
    if periods_to_create:
        LegalUnitPeriod.objects.bulk_create(periods_to_create, ignore_conflicts=True)

    with STATE.lock:
        STATE.processed_records += len(batch)
        total_records = STATE.total_records
        processed_records = STATE.processed_records
        STATE.current_batch_speed = len(batch) / max(time.perf_counter() - batch_start, 1e-9)
        STATE.average_speed = (
            STATE.current_batch_speed
            if STATE.average_speed == 0
            else (STATE.average_speed * 0.9 + STATE.current_batch_speed * 0.1)
        )

    batch_elapsed = max(time.perf_counter() - batch_start, 1e-9)
    batch_speed = len(batch) / batch_elapsed
    percentage = 0 if total_records == 0 else (processed_records / total_records) * 100

    print(
        f"Batch: {len(batch)} units | "
        f"Legal units: {len(legal_units_to_create)} created, {len(legal_units_to_update)} updated | "
        f"Periods inserted: {len(periods_to_create)} | "
        f"Speed: {batch_speed:.2f} units/s | "
        f"Progress: {processed_records}/{total_records} ({percentage:.2f}%)"
    )


def init_progress_chart():
    global PROGRESS_FIG, PROGRESS_AX, PROGRESS_BARS, PROGRESS_TEXT
    plt.ion()
    PROGRESS_FIG, PROGRESS_AX = plt.subplots(figsize=(10, 5))
    with STATE.lock:
        total_records = STATE.total_records
    PROGRESS_BARS = PROGRESS_AX.bar(
        ["Processed", "Remaining"],
        [0, max(total_records, 0)],
        color=["blue", "orange"],
    )
    PROGRESS_AX.set_title("SIREN API Processing Progress")
    PROGRESS_AX.set_ylabel("Number of Records")
    PROGRESS_AX.set_ylim(0, max(total_records, 1))
    PROGRESS_TEXT = PROGRESS_AX.text(
        0.5,
        0.95,
        "0%",
        transform=PROGRESS_AX.transAxes,
        ha="center",
        va="top",
    )
    PROGRESS_FIG.canvas.draw()
    PROGRESS_FIG.canvas.flush_events()
    plt.show(block=False)


def plot_progress():
    global PROGRESS_FIG, PROGRESS_BARS, PROGRESS_TEXT
    if PROGRESS_FIG is None or PROGRESS_BARS is None:
        init_progress_chart()

    with STATE.lock:
        processed_records = STATE.processed_records
        total_records = STATE.total_records
        process_start_time = STATE.process_start_time
        current_batch_speed = STATE.current_batch_speed
        batch_size = STATE.batch_size

    remaining = max(total_records - processed_records, 0)
    PROGRESS_BARS[0].set_height(processed_records)
    PROGRESS_BARS[1].set_height(remaining)

    percentage = 0 if total_records == 0 else (processed_records / total_records) * 100
    elapsed_seconds = 0 if process_start_time is None else (time.perf_counter() - process_start_time)
    rows_per_second = 0 if elapsed_seconds <= 0 else (processed_records / elapsed_seconds)
    PROGRESS_TEXT.set_text(
        f"{percentage:.2f}% | "
        f"Elapsed: {elapsed_seconds:.1f}s | "
        f"Avg: {rows_per_second:.2f} rows/s | "
        f"Batch: {current_batch_speed:.2f} rows/s | "
        f"Batch size: {batch_size} rows"
    )

    PROGRESS_FIG.canvas.draw_idle()
    PROGRESS_FIG.canvas.flush_events()
    plt.pause(0.05)


def process_worker():
    process_request_in_batch(processor=process_batch)
    with STATE.lock:
        STATE.is_done = True


def main():
    print("Starting legal unit fetch from SIREN API...")
    count = Business.objects.count()
    with STATE.lock:
        STATE.total_records = count
    print(f"Total businesses in DB: {STATE.total_records}")
    print(f"Processing in batches... (Size: {STATE.batch_size})")

    init_progress_chart()

    worker = threading.Thread(target=process_worker, daemon=False)
    worker.start()

    while worker.is_alive():
        plot_progress()

    worker.join()
    plot_progress()
    plt.ioff()
    plt.show()


if __name__ == "__main__":
    main()
