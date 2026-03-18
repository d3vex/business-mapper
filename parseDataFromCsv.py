import csv
import matplotlib.pyplot as plt
import django
from django.contrib.gis.geos import Point
import time
import os, sys
import threading
from dataclasses import dataclass, field

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'sirene.settings')
django.setup()

from business.models import Business, Batiment, LegalUnit

PROGRESS_FIG = None
PROGRESS_AX = None
PROGRESS_BARS = None
PROGRESS_TEXT = None


@dataclass
class ProcessingState:
    total_records: int = 0
    processed_records: int = 0
    process_start_time: float | None = None
    current_batch_speed: float = 0.0
    average_speed: float = 0.0
    old_average_speed: float = 0.0
    is_done: bool = False
    batch_size: int = 2000  # Increased from 128 for better bulk operation performance
    optimize_direction: int = 0  # 0 = no change, 1 = increase, -1 = decrease
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)


STATE = ProcessingState()

def iter_csv_batches(file_path):
    """Yield batches where batch is a list of dicts mapping header->value."""
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        batch = []
        for row in reader:
            if not row or not row.get('siret'):
                continue
            batch.append(row)
            if len(batch) >= STATE.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

def process_csv_in_batches(file_path, processor=None):
    """
    Iterate CSV by batches. If processor is provided, it will be called as:
        processor(batch)
    where batch is a list of dicts.
    """
    for batch in iter_csv_batches(file_path):
        if processor:
            processor(batch)
        else:
            # default action: simple summary per batch
            print(f"Batch processed: {len(batch)} rows")


def read_csv(batch):
    """Process a batch of CSV rows into database records efficiently."""
    batch_start_time = time.perf_counter()
    with STATE.lock:
        if STATE.process_start_time is None:
            STATE.process_start_time = batch_start_time

    # Extract all sirets from batch
    sirets_in_batch = [row.get('siret') for row in batch if row.get('siret')]
    
    # Single query to get existing locations by siret
    existing_sirets = set(
        Batiment.objects.filter(siret__in=sirets_in_batch).values_list('siret', flat=True)
    )

    businesses_to_create = {}  # siren -> Business object
    locations_to_create = []
    legal_units_to_create = {}  # siren -> LegalUnit object
    
    # Track deduplicates within batch (in case CSV has duplicates)
    seen_sirets = set()

    for row in batch:
        siret = row.get('siret')
        if not siret or siret in existing_sirets or siret in seen_sirets:
            continue
        
        seen_sirets.add(siret)
        siren = siret[:9]

        # Create Business if not already created in this batch
        if siren not in businesses_to_create:
            business = Business(siren=siren)
            businesses_to_create[siren] = business

        # Create Batiment (physical establishment with siret)
        try:
            batiment_obj = Batiment(
                business_id=siren,
                siret=siret,
                postal_code=row.get('plg_code_commune'),
                location=Point(
                    float(row.get('x_longitude', 0)),
                    float(row.get('y_latitude', 0)),
                    srid=4326
                )
            )
            locations_to_create.append(batiment_obj)
        except (ValueError, TypeError) as e:
            print(f"Invalid coordinates for SIRET {siret}: x={row.get('x_longitude')}, y={row.get('y_latitude')}", e)
            # Skip if coordinates are invalid
            pass

        # Create LegalUnit if not already created in this batch
        if siren not in legal_units_to_create:
            legal_unit = LegalUnit(
                siren=siren,
                business_id=siren,
            )
            legal_units_to_create[siren] = legal_unit

    # Bulk create all at once
    if businesses_to_create:
        Business.objects.bulk_create(
            businesses_to_create.values(),
            batch_size=1000,
            ignore_conflicts=True
        )
    if locations_to_create:
        Batiment.objects.bulk_create(locations_to_create, batch_size=1000, ignore_conflicts=True)
    if legal_units_to_create:
        LegalUnit.objects.bulk_create(
            legal_units_to_create.values(),
            batch_size=1000,
            ignore_conflicts=True
        )

    batch_elapsed = max(time.perf_counter() - batch_start_time, 1e-9)
    batch_speed = len(batch) / batch_elapsed

    with STATE.lock:
        STATE.processed_records += len(batch)
        STATE.current_batch_speed = batch_speed
        STATE.average_speed = (
            batch_speed
            if STATE.average_speed == 0
            else (STATE.average_speed * 0.9 + batch_speed * 0.1)
        )
        processed_records = STATE.processed_records
        total_records = STATE.total_records

    percentage = 0 if total_records == 0 else (processed_records / total_records) * 100
    print(
        f"Processed {processed_records}/{total_records} rows "
        f"({percentage:.2f}%) - "
        f"Batch speed: {batch_speed:.2f} rows/s - "
        f"Created: {len(businesses_to_create)} businesses, "
        f"{len(locations_to_create)} locations"
    )


def count_lines(file_path):
    with open(file_path, 'r') as f:
        return sum(1 for _ in f)


def init_progress_chart():
    global PROGRESS_FIG, PROGRESS_AX, PROGRESS_BARS, PROGRESS_TEXT
    plt.ion()
    PROGRESS_FIG, PROGRESS_AX = plt.subplots(figsize=(10, 5))
    with STATE.lock:
        total_records = STATE.total_records
    PROGRESS_BARS = PROGRESS_AX.bar(
        ['Processed', 'Remaining'],
        [0, max(total_records, 0)],
        color=['blue', 'orange']
    )
    PROGRESS_AX.set_title('CSV Processing Progress')
    PROGRESS_AX.set_ylabel('Number of Records')
    PROGRESS_AX.set_ylim(0, max(total_records, 1))
    PROGRESS_TEXT = PROGRESS_AX.text(
        0.5,
        0.95,
        '0%',
        transform=PROGRESS_AX.transAxes,
        ha='center',
        va='top'
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


def process_worker(file_path):
    process_csv_in_batches(file_path, processor=read_csv)
    with STATE.lock:
        STATE.is_done = True

def batch_size_optimizer():
    with STATE.lock:
        if STATE.average_speed > STATE.old_average_speed:
            optimal_batch_size = int(STATE.average_speed * (1.4 * STATE.batch_size / max(STATE.current_batch_speed, 1e-9)))
            optimal_batch_size = min(10000, max(optimal_batch_size, 10))
            if optimal_batch_size != STATE.batch_size:
                STATE.batch_size = optimal_batch_size
            STATE.old_average_speed = STATE.average_speed
            return
        if STATE.average_speed < STATE.old_average_speed:
            optimal_batch_size = int(STATE.average_speed * (0.5 * STATE.batch_size / max(STATE.current_batch_speed, 1e-9)))
            optimal_batch_size = min(10000, max(optimal_batch_size, 10))
            if optimal_batch_size != STATE.batch_size:
                STATE.batch_size = optimal_batch_size
            STATE.old_average_speed = STATE.average_speed


if __name__ == "__main__":
    print("Starting CSV processing...")
    print("Counting total lines in CSV...")
    path = '../GeolocalisationEtablissement_Sirene_pour_etudes_statistiques_utf8.csv'
    with STATE.lock:
        STATE.total_records = max(count_lines(path) - 1, 0)
    print(f"Total lines in CSV: {STATE.total_records}")
    print(f"Processing CSV in batches... (Size: {STATE.batch_size})")

    init_progress_chart()

    worker = threading.Thread(target=process_worker, args=(path,), daemon=False)
    worker.start()

    while worker.is_alive():
        plot_progress()

    worker.join()
    plot_progress()
    plt.ioff()
