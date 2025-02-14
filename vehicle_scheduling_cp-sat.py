"""Bus scheduling example using CP-SAT with CSV occupancy input.

Time is discretized into 48 half-hour intervals:
  Slot 0: 00:00-00:29, Slot 1: 00:30-00:59, ..., Slot 47: 23:30-23:59.
Only departures from slot 10 (05:00-05:29) to slot 47 (23:30-23:59) are allowed.
Travel times (in slots) depend on the departure slot:
  - Morning (slots 10-23):   6 hrs  → 12 slots
  - Afternoon (slots 24-35): 7 hrs  → 14 slots
  - Evening (slots 36-43):   8 hrs  → 16 slots
  - Late Night (slots 44-47): 6 hrs  → 12 slots

Each bus must run at least 2 trips (a round-trip) and may run an optional 3rd trip.
Occupancy values (from CSV) are assumed to be in [0,1] and are scaled by 1000.

The objective is to maximize total (scaled) occupancy over the chosen departures.
"""

import csv
from ortools.sat.python import cp_model

# Load occupancy data from CSV.
# The CSV file should have 48 rows (one per slot 0 to 47) with two columns:
#   slot, occupancy
# Occupancy values are assumed to be in the 0-1 range.
# We scale them by 1000 to convert to integer values.
occupancy = [0] * 48
with open('occupancy.csv', 'r') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        try:
            slot = int(row[0])
            occ = float(row[1])
            occupancy[slot] = int(occ * 1000)
        except ValueError:
            continue  # Skip header or malformed lines

# Allowed departure slots: Only allow departures between 05:00–05:29 (slot 10) and 23:30–23:59 (slot 47).
ALLOWED_SLOTS = list(range(10, 48))

# Precompute travel times for each slot (in half-hour units).
# (If a bus departs at a given slot, the travel time is predetermined.)
travel_time = [0] * 48
for t in range(48):
    if 10 <= t <= 23:      # Morning: 05:00-11:29 → 6 hrs = 12 slots
        travel_time[t] = 12
    elif 24 <= t <= 35:    # Afternoon: 12:00-17:29 → 7 hrs = 14 slots
        travel_time[t] = 14
    elif 36 <= t <= 43:    # Evening: 18:00-21:29 → 8 hrs = 16 slots
        travel_time[t] = 16
    elif 44 <= t <= 47:    # Late night: 22:00-23:59 → 6 hrs = 12 slots
        travel_time[t] = 12
    else:
        travel_time[t] = 0

# Parameters
NUM_BUSES = 10
NUM_TRIPS = 3  # Trips 0 and 1 are mandatory; trip 2 is optional.
# Define an occupancy threshold (scaled value).
# For example, if you want only slots with occupancy >= 0.3 (i.e. 300 after scaling)
occupancy_threshold = 300

# For printing, assign starting locations: first half the buses start in Bangalore,
# the remaining buses in Tirupati.
starting_location = {}
for i in range(NUM_BUSES):
    starting_location[i] = "Bangalore" if i < NUM_BUSES // 2 else "Tirupati"

def destination_for(bus, trip_index):
    """
    For buses starting in Bangalore:
       Trip 0: Bangalore -> Tirupati,
       Trip 1: Tirupati -> Bangalore,
       Trip 2: Bangalore -> Tirupati.
    For buses starting in Tirupati, the reverse.
    """
    start = starting_location[bus]
    if start == "Bangalore":
        return "Tirupati" if trip_index % 2 == 0 else "Bangalore"
    else:
        return "Bangalore" if trip_index % 2 == 0 else "Tirupati"

def slot_interval_string(slot):
    """Return a string representing the half-hour interval for a given slot.
    E.g., slot 0 -> "00:00-00:29", slot 1 -> "00:30-00:59", etc.
    """
    start_hour = slot // 2
    start_minute = (slot % 2) * 30
    # End time is 29 minutes after the start.
    end_hour = start_hour
    end_minute = start_minute + 29
    # Adjust if minutes exceed 59.
    if end_minute >= 60:
        end_minute -= 60
        end_hour += 1
    return f"{start_hour:02d}:{start_minute:02d}-{end_hour:02d}:{end_minute:02d}"

def main():
    model = cp_model.CpModel()
    
    # Decision variables:
    # x[(i, j, t)] = 1 if bus i, trip j starts at time slot t.
    x = {}
    for i in range(NUM_BUSES):
        for j in range(NUM_TRIPS):
            for t in ALLOWED_SLOTS:
                x[(i, j, t)] = model.NewBoolVar(f"x_{i}_{j}_{t}")
    
    # Each bus must have exactly one departure for trips 0 and 1 (mandatory)
    for i in range(NUM_BUSES):
        for j in range(2):
            model.Add(sum(x[(i, j, t)] for t in ALLOWED_SLOTS) == 1)
        # Trip 2 is optional.
        model.Add(sum(x[(i, 2, t)] for t in ALLOWED_SLOTS) <= 1)
    
    # Enforce occupancy threshold: force x[(i,j,t)] == 0 if occupancy at slot t is below threshold.
    for i in range(NUM_BUSES):
        for j in range(NUM_TRIPS):
            for t in ALLOWED_SLOTS:
                if occupancy[t] < occupancy_threshold:
                    model.Add(x[(i, j, t)] == 0)
    
    # Sequencing constraints:
    # If bus i runs trip j at time slot t and trip j+1 at slot t_next,
    # then t_next must be at least t + travel_time[t] + 4 (4 slots = 2 hours layover).
    for i in range(NUM_BUSES):
        for j in range(NUM_TRIPS - 1):
            for t in ALLOWED_SLOTS:
                gap = travel_time[t] + 4
                for t_next in ALLOWED_SLOTS:
                    if t_next < t + gap:
                        model.Add(x[(i, j, t)] + x[(i, j+1, t_next)] <= 1)
    
    # Objective: maximize total occupancy.
    # (Each departure at slot t earns occupancy[t], which is scaled by 1000.)
    total_occupancy = []
    for i in range(NUM_BUSES):
        for j in range(NUM_TRIPS):
            for t in ALLOWED_SLOTS:
                total_occupancy.append(occupancy[t] * x[(i, j, t)])
    model.Maximize(sum(total_occupancy))
    
    # Solve the model.
    solver = cp_model.CpSolver()
    status = solver.Solve(model)
    
    if status in [cp_model.OPTIMAL, cp_model.FEASIBLE]:
        print("Optimal bus schedule:\n")
        for i in range(NUM_BUSES):
            print(f"Bus {i} (starts at {starting_location[i]}):")
            for j in range(NUM_TRIPS):
                chosen_slot = None
                for t in ALLOWED_SLOTS:
                    if solver.Value(x[(i, j, t)]) == 1:
                        chosen_slot = t
                        break
                if chosen_slot is not None:
                    depart_interval = slot_interval_string(chosen_slot)
                    # Compute arrival slot: departure slot + travel_time.
                    arrival_slot = chosen_slot + travel_time[chosen_slot]
                    arrival_interval = slot_interval_string(arrival_slot % 48)  # Wrap around if next day.
                    # Determine source and destination:
                    if j == 0:
                        src = starting_location[i]
                    else:
                        # For trip j, source is the destination of trip j-1.
                        src = destination_for(i, j-1)
                    dst = destination_for(i, j)
                    occ = occupancy[chosen_slot] / 1000  # Convert back to 0-1 range for display.
                    print(f"  Trip {j}: {src} -> {dst}, depart {depart_interval}, arrive {arrival_interval}, occupancy = {occ:.3f}")
                else:
                    print(f"  Trip {j}: not scheduled")
            print()
        print("Objective (total scaled occupancy):", solver.ObjectiveValue())
    else:
        print("No solution found.")

if __name__ == "__main__":
    main()
