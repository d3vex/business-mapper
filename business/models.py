from django.db import models
from django.contrib.gis.db import models as gis_models


class Business(models.Model):
    """Represents an enterprise/company identified by SIREN (9 digits)"""
    siren = models.CharField(max_length=9, primary_key=True)

    class Meta:
        db_table = 'enterprise'

    def __str__(self):
        return f"SIREN {self.siren}"


class Batiment(models.Model):
    """Represents a physical building/establishment identified by SIRET (14 digits)"""

    business = models.ForeignKey(
        Business,
        on_delete=models.CASCADE,
        related_name="batiments",
        db_column="business_siren",
        to_field="siren",
    )
    siret = models.CharField(max_length=14, unique=True, primary_key=True)

    postal_code = models.CharField(max_length=10, null=True, blank=True)

    location = gis_models.PointField(
        srid=4326,
        null=True,
        blank=True
    )

    class Meta:
        db_table = "batiment"

    def __str__(self):
        return f"SIRET {self.siret} - {self.postal_code}"


class LegalUnit(models.Model):
    """Represents a legal unit identified by SIREN"""
    siren = models.CharField(max_length=9, primary_key=True)
    
    business = models.OneToOneField(
        Business,
        on_delete=models.CASCADE,
        related_name="legal_unit",
        db_column="business_siren",
        to_field="siren",
    )

    # Core metadata
    statut_diffusion = models.CharField(max_length=1, null=True, blank=True)  # "O" or "P"
    unite_purgee = models.BooleanField(null=True, blank=True)
    date_creation = models.DateField(null=True, blank=True)

    # Personal info (when applicable)
    sexe = models.CharField(max_length=1, null=True, blank=True)  # "M" or "F"
    prenom1 = models.CharField(max_length=255, null=True, blank=True)
    prenom2 = models.CharField(max_length=255, null=True, blank=True)
    prenom3 = models.CharField(max_length=255, null=True, blank=True)
    prenom4 = models.CharField(max_length=255, null=True, blank=True)
    prenom_usuel = models.CharField(max_length=255, null=True, blank=True)
    pseudonyme = models.CharField(max_length=255, null=True, blank=True)
    identifiant_association = models.CharField(max_length=255, null=True, blank=True)

    # Employee info
    tranche_effectifs = models.CharField(max_length=10, null=True, blank=True)  # e.g., "NN"
    annee_effectifs = models.CharField(max_length=4, null=True, blank=True)

    # Processing info
    date_dernier_traitement = models.DateTimeField(null=True, blank=True)
    nombre_periodes = models.IntegerField(default=0)

    # Enterprise category
    categorie_entreprise = models.CharField(max_length=50, null=True, blank=True)
    annee_categorie_entreprise = models.CharField(max_length=4, null=True, blank=True)
    activite_principale_naf25 = models.CharField(max_length=10, null=True, blank=True)

    class Meta:
        db_table = 'legal_unit'
        indexes = [
            models.Index(fields=['statut_diffusion']),
            models.Index(fields=['prenom1', 'prenom_usuel']),
        ]

    def __str__(self):
        return f"SIREN {self.siren}"


class LegalUnitPeriod(models.Model):
    """
    Represents a historical period for a legal unit.
    Each period contains business details valid for a specific date range.
    """
    legal_unit = models.ForeignKey(
        LegalUnit,
        on_delete=models.CASCADE,
        related_name='periods',
        to_field='siren'
    )

    # Date range for this period
    date_debut = models.DateField(null=True, blank=True)
    date_fin = models.DateField(null=True, blank=True)

    # Administrative state
    etat_administratif = models.CharField(max_length=1, null=True, blank=True)  # "A", "C", etc.
    changement_etat_administratif = models.BooleanField(default=False)

    # Names and denominations
    nom = models.CharField(max_length=255, null=True, blank=True)
    changement_nom = models.BooleanField(default=False)
    nom_usage = models.CharField(max_length=255, null=True, blank=True)
    changement_nom_usage = models.BooleanField(default=False)
    denomination = models.CharField(max_length=255, null=True, blank=True)
    changement_denomination = models.BooleanField(default=False)

    # Usual denominations (alternate names)
    denomination_usuelle1 = models.CharField(max_length=255, null=True, blank=True)
    denomination_usuelle2 = models.CharField(max_length=255, null=True, blank=True)
    denomination_usuelle3 = models.CharField(max_length=255, null=True, blank=True)
    changement_denomination_usuelle = models.BooleanField(default=False)

    # Legal category
    categorie_juridique = models.CharField(max_length=10, null=True, blank=True)
    changement_categorie_juridique = models.BooleanField(default=False)

    # Activity
    activite_principale = models.CharField(max_length=10, null=True, blank=True)
    nomenclature_activite_principale = models.CharField(max_length=20, null=True, blank=True)
    changement_activite_principale = models.BooleanField(default=False)

    # Establishment (NIC)
    nic_siege = models.CharField(max_length=10, null=True, blank=True)
    changement_nic_siege = models.BooleanField(default=False)

    # Social economy and mission
    economie_sociale_solidaire = models.CharField(max_length=1, null=True, blank=True)
    changement_economie_sociale_solidaire = models.BooleanField(default=False)
    societe_mission = models.CharField(max_length=1, null=True, blank=True)
    changement_societe_mission = models.BooleanField(default=False)

    # Employment status
    caractere_employeur = models.CharField(max_length=1, null=True, blank=True)
    changement_caractere_employeur = models.BooleanField(default=False)

    class Meta:
        db_table = 'legal_unit_period'
        ordering = ['-date_debut']  # Most recent first
        indexes = [
            models.Index(fields=['legal_unit', 'date_debut']),
            models.Index(fields=['date_debut', 'date_fin']),
            models.Index(fields=['nom', 'denomination']),
            models.Index(fields=['activite_principale']),
        ]

    def __str__(self):
        period_str = f"{self.date_debut or 'N/A'} → {self.date_fin or 'Present'}"
        return f"{self.legal_unit.siren} | {period_str}"
