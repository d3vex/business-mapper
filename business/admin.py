from django.contrib import admin
from .models import Business, LegalUnit, LegalUnitPeriod, Batiment
admin.site.register(Business)
admin.site.register(Batiment)
admin.site.register(LegalUnit)
admin.site.register(LegalUnitPeriod)