from django.urls import path

from . import views

app_name = "business"

urlpatterns = [
    path("dashboard/", views.dashboard_view, name="dashboard"),
    path("dashboard/map-data/", views.dashboard_map_data, name="dashboard_map_data"),
    path("dashboard/naf-codes/", views.dashboard_naf_codes, name="dashboard_naf_codes"),
]
