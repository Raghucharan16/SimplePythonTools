import csv
from ortools.sat.python import cp_model
import datetime

# Utility: convert time string (e.g. "05:00 AM") to minutes since midnight.
def time_str_to_minutes(time_str):
    dt = datetime.datetime.strptime(time_str, "%I:%M %p")
    return dt.hour * 60 + dt.minute

# Utility: convert minutes to time string (wraps over 24 hrs)
def minutes_to_time_str(m):
    m_mod = m % 1440  # wrap around 24 hours
    h = m_mod // 60
    m_rem = m_mod % 60
    period = "AM" if h < 12 else "PM"
    if h == 0:
        h = 12
    elif h > 12:
        h = h - 12
    return f"{h:02d}:{m_rem:02d} {period}"

# Load CSV and split candidate departure options by route direction.
def load_csv(filename):
    options_AB = []  # For trips from Bangalore to Tirupati.
    options_BA = []  # For trips from Tirupati to Bangalore.
    with open(filename, 'r', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            route = row['route'].strip()
            time_str = row['journey_time'].strip()
            occ = float(row['Occupancy Rate'])
            t = time_str_to_minutes(time_str)
            if route.startswith("Bangalore"):
                options_AB.append((t, occ))
            elif route.startswith("Tirupati"):
                options_BA.append((t, occ))
    options_AB.sort(key=lambda x: x[0])
    options_BA.sort(key=lambda x: x[0])
    return options_AB, options_BA

def main():
    # Load data.
    filename = "combined_occupancy.csv"  # CSV file with columns: route, journey_time, Occupancy Rate.
    options_AB, options_BA = load_csv(filename)
    
    # Candidate departure times and occupancy scores for each direction.
    times_AB = [opt[0] for opt in options_AB]
    occ_AB = [int(opt[1] * 1000) for opt in options_AB]  # scale occupancy to integer
    times_BA = [opt[0] for opt in options_BA]
    occ_BA = [int(opt[1] * 1000) for opt in options_BA]

    # Parameters (in minutes since midnight)
    earliest = 4 * 60          # 4:00 AM = 240
    latest  = 23 * 60 + 55       # 11:55 PM = 1435
    trip_duration = 480          # Each leg takes 480 minutes (8 hours)
    layover = 120                # Additional layover time (e.g. 2 hours)
    round_trip_max = 1440        # A bus must complete its round trip within 24 hrs.
    
    # Parameter for ensuring a minimum gap between the start times of the same leg among buses.
    min_gap_between_buses = 30  # e.g., at least 30 minutes between any two buses starting a leg

    num_buses = 10  
    # We assign half the buses to start at Bangalore (Group AB) and half at Tirupati (Group BA)
    group_AB = list(range(0, num_buses // 2))
    group_BA = list(range(num_buses // 2, num_buses))
    
    model = cp_model.CpModel()
    
    # Decision variables:
    # For each bus, we select an outbound departure option (index into candidate list)
    # and a return departure option (from the candidate list for the opposite direction).
    x = {}         # index selection variables
    dep = {}       # actual departure time variables (in minutes)
    occ_vars = {}  # occupancy values from the chosen candidate
    
    # For buses in group_AB: outbound from Bangalore (using times_AB), return from Tirupati (using times_BA)
    for i in group_AB:
        # Outbound leg:
        var_name = f"bus{i}_outbound"
        x[i, 0] = model.NewIntVar(0, len(times_AB) - 1, var_name)
        dep[i, 0] = model.NewIntVar(earliest, latest, f"dep_{var_name}")
        model.AddElement(x[i, 0], times_AB, dep[i, 0])
        occ_vars[i, 0] = model.NewIntVar(0, 10000, f"occ_{var_name}")
        model.AddElement(x[i, 0], occ_AB, occ_vars[i, 0])
        
        # Return leg:
        var_name = f"bus{i}_return"
        x[i, 1] = model.NewIntVar(0, len(times_BA) - 1, var_name)
        dep[i, 1] = model.NewIntVar(earliest, latest, f"dep_{var_name}")
        model.AddElement(x[i, 1], times_BA, dep[i, 1])
        occ_vars[i, 1] = model.NewIntVar(0, 10000, f"occ_{var_name}")
        model.AddElement(x[i, 1], occ_BA, occ_vars[i, 1])
        
        # Ensure return leg starts after outbound leg finishes plus layover.
        model.Add(dep[i, 1] >= dep[i, 0] + trip_duration + layover)
        # Ensure the whole round trip (arrival of return leg) is within 24 hours from outbound departure.
        model.Add(dep[i, 1] + trip_duration <= dep[i, 0] + round_trip_max)
    
    # For buses in group_BA: outbound from Tirupati (using times_BA), return from Bangalore (using times_AB)
    for i in group_BA:
        # Outbound leg:
        var_name = f"bus{i}_outbound"
        x[i, 0] = model.NewIntVar(0, len(times_BA) - 1, var_name)
        dep[i, 0] = model.NewIntVar(earliest, latest, f"dep_{var_name}")
        model.AddElement(x[i, 0], times_BA, dep[i, 0])
        occ_vars[i, 0] = model.NewIntVar(0, 10000, f"occ_{var_name}")
        model.AddElement(x[i, 0], occ_BA, occ_vars[i, 0])
        
        # Return leg:
        var_name = f"bus{i}_return"
        x[i, 1] = model.NewIntVar(0, len(times_AB) - 1, var_name)
        dep[i, 1] = model.NewIntVar(earliest, latest, f"dep_{var_name}")
        model.AddElement(x[i, 1], times_AB, dep[i, 1])
        occ_vars[i, 1] = model.NewIntVar(0, 10000, f"occ_{var_name}")
        model.AddElement(x[i, 1], occ_AB, occ_vars[i, 1])
        
        model.Add(dep[i, 1] >= dep[i, 0] + trip_duration + layover)
        model.Add(dep[i, 1] + trip_duration <= dep[i, 0] + round_trip_max)
    
    # Enforce that no two buses in the same group start the outbound leg at the same time.
    model.AddAllDifferent([dep[i, 0] for i in group_AB])
    model.AddAllDifferent([dep[i, 0] for i in group_BA])
    
    # Also enforce that no two buses in the same group have the same return departure time.
    model.AddAllDifferent([dep[i, 1] for i in group_AB])
    model.AddAllDifferent([dep[i, 1] for i in group_BA])
    
    # Additionally, if you want a minimum gap (e.g. 30 minutes) between any two departure times within each group,
    # add pairwise constraints using auxiliary variables.
    def add_min_gap_constraints(bus_list, leg):
        for idx1 in range(len(bus_list)):
            for idx2 in range(idx1+1, len(bus_list)):
                i = bus_list[idx1]
                j = bus_list[idx2]
                diff = model.NewIntVar(-latest, latest, f"diff_{i}_{j}_leg{leg}")
                model.Add(diff == dep[i, leg] - dep[j, leg])
                abs_diff = model.NewIntVar(0, latest, f"abs_diff_{i}_{j}_leg{leg}")
                model.AddAbsEquality(abs_diff, diff)
                model.Add(abs_diff >= min_gap_between_buses)
    add_min_gap_constraints(group_AB, 0)
    add_min_gap_constraints(group_AB, 1)
    add_min_gap_constraints(group_BA, 0)
    add_min_gap_constraints(group_BA, 1)
    
    # Objective: maximize overall occupancy.
    total_occ = model.NewIntVar(0, 100000, "total_occ")
    model.Add(total_occ == sum(occ_vars[i, j] for i in range(num_buses) for j in [0, 1]))
    model.Minimize(-total_occ)
    
    # Solve the model.
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 60.0
    status = solver.Solve(model)
    
    if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        print("Solution found:")
        for i in range(num_buses):
            out = solver.Value(dep[i, 0])
            ret = solver.Value(dep[i, 1])
            arr_out = out + trip_duration
            arr_ret = ret + trip_duration
            occ_out = solver.Value(occ_vars[i, 0]) / 1000.0
            occ_ret = solver.Value(occ_vars[i, 1]) / 1000.0
            if i in group_AB:
                start = "Bangalore"
                dest = "Tirupati"
            else:
                start = "Tirupati"
                dest = "Bangalore"
            print(f"\nBus {i} (starting from {start}):")
            print(f"  Outbound: Depart at {minutes_to_time_str(out)} (Occupancy: {occ_out:.3f}) -> Arrives at {dest} at {minutes_to_time_str(arr_out)}")
            print(f"  Return:   Depart at {minutes_to_time_str(ret)} (Occupancy: {occ_ret:.3f}) -> Arrives at {start} at {minutes_to_time_str(arr_ret)}")
            # Check round-trip constraint:
            if arr_ret > out + 1440:
                print("  *** Round-trip exceeds 24 hours!")
        print("\nTotal occupancy (scaled):", solver.Value(total_occ))
    else:
        print("No solution found.")

if __name__ == "__main__":
    main()
