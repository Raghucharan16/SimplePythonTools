import pandas as pd
from ortools.sat.python import cp_model

# -----------------------------
# Step 1. Data Ingestion & Preprocessing
# -----------------------------

csv_file = 'basic_occupancy.csv'
df = pd.read_csv(csv_file)

def time_to_minutes(time_str):
    hours, minutes = map(int, time_str.split(':'))
    return hours * 60 + minutes

df['Original_Start_Min'] = df['Start_Time'].apply(time_to_minutes)

# -----------------------------
# Step 2. Model Formulation with OR-Tools CP-SAT
# -----------------------------
EARLIEST_START = 5 * 60            # 5:00 AM => 300 minutes
LATEST_END = 23 * 60 + 50          # 11:50 PM => 1430 minutes
TRIP_DURATION = 7 * 60             # 7 hours = 420 minutes
LAYOVER = 2 * 60                   # 2 hours = 120 minutes
SERVICE_BLOCK = TRIP_DURATION + LAYOVER  # 540 minutes

TARGET_TIME = 9 * 60  # Target departure: 9:00 AM (540 minutes)
ALLOWED_DEVIATION = 30

model = cp_model.CpModel()

new_start_vars = {}
deviation_vars = {}
score_vars = {}
max_score = 1000

for idx, row in df.iterrows():
    var = model.NewIntVar(EARLIEST_START, LATEST_END, f'new_start_{idx}')
    new_start_vars[idx] = var
    orig = row['Original_Start_Min']
    model.Add(var >= orig - ALLOWED_DEVIATION)
    model.Add(var <= orig + ALLOWED_DEVIATION)
    
    max_deviation = LATEST_END - EARLIEST_START
    deviation = model.NewIntVar(0, max_deviation, f'deviation_{idx}')
    deviation_vars[idx] = deviation
    model.Add(deviation >= var - TARGET_TIME)
    model.Add(deviation >= TARGET_TIME - var)
    
    score = model.NewIntVar(0, max_score, f'score_{idx}')
    score_vars[idx] = score
    model.Add(score == max_score - deviation)

# Use the built-in 'indices' property to get a dictionary mapping bus IDs to row indices.
bus_services = {bus: list(indices) for bus, indices in df.groupby('Id').indices.items()}
for bus, indices in bus_services.items():
    if len(indices) == 2:
        morning, evening = sorted(indices, key=lambda i: df.loc[i, 'Original_Start_Min'])
        model.Add(new_start_vars[evening] >= new_start_vars[morning] + SERVICE_BLOCK)

model.Maximize(sum(score_vars[idx] for idx in score_vars))

# -----------------------------
# Step 3. Solve the Model
# -----------------------------
solver = cp_model.CpSolver()
status = solver.Solve(model)

# -----------------------------
# Step 4. Postprocessing & Saving Output
# -----------------------------
def minutes_to_time(minutes):
    hours = minutes // 60
    mins = minutes % 60
    return f'{hours:02d}:{mins:02d}'

if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
    results = []
    for idx, row in df.iterrows():
        optimized_start = solver.Value(new_start_vars[idx])
        results.append({
            'Id': row['Id'],
            'Service_Number': row['Service_Number'],
            'Original_Start_Time': row['Start_Time'],
            'Optimized_Start_Min': optimized_start,
            'Optimized_Start_Time': minutes_to_time(optimized_start),
            'Occupancy': row['Occupancy'],
            'Deviation_from_Target': solver.Value(deviation_vars[idx]),
            'Score': solver.Value(score_vars[idx])
        })
        print(f"Service {row['Service_Number']} (Bus {row['Id']}): {minutes_to_time(optimized_start)} "
              f"(Deviation: {solver.Value(deviation_vars[idx])} minutes, Score: {solver.Value(score_vars[idx])})")
    
    result_df = pd.DataFrame(results)
    output_csv = 'optimized_bus_schedule.csv'
    result_df.to_csv(output_csv, index=False)
    print(f"\nOptimized schedule saved to {output_csv}")
else:
    print("No feasible solution found.")
