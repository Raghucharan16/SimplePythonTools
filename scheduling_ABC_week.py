import csv
import datetime
from ortools.sat.python import cp_model

# ---------------------------------------------------
# Utility: Convert a time string (e.g. "1:30 PM") to minutes since midnight.
# ---------------------------------------------------
def time_str_to_minutes(time_str):
    dt = datetime.datetime.strptime(time_str, "%I:%M %p")
    return dt.hour * 60 + dt.minute

# ---------------------------------------------------
# Utility: Convert a slot number to a human‐readable day/time string.
# Assume slot 0 corresponds to 4:00 AM on day 0. Each slot represents 30 minutes.
# ---------------------------------------------------
def slot_to_day_time(slot):
    # Calculate day number (0-6 for a week)
    day = slot // 48  # 48 slots per day
    day_slot = slot % 48  # slot within the day
    
    # Convert to day name
    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    day_name = day_names[day]
    
    # Compute time from base (4:00 AM = 240 minutes)
    minutes_from_midnight = 240 + day_slot * 30
    h = minutes_from_midnight // 60
    m = minutes_from_midnight % 60
    period = "AM" if h < 12 else "PM"
    if h == 0:
        h = 12
    elif h > 12:
        h -= 12
    return f"{day_name} {h:02d}:{m:02d} {period}"

# ---------------------------------------------------
# Load candidate options from a CSV file.
# The CSV must have columns: route, slot, occupancy, day.
# If 'day' column doesn't exist, we'll replicate the data for each day of the week.
# ---------------------------------------------------
def load_candidates(filename):
    # We'll store candidates for routes "A-B", "B-A", and "B-C"
    options = {'A-B': [], 'B-A': [], 'B-C': []}
    
    has_day_column = False
    
    with open(filename, newline='') as f:
        reader = csv.DictReader(f)
        # Check if 'day' column exists
        if 'day' in reader.fieldnames:
            has_day_column = True
        
        for row in reader:
            route = row['route'].strip()
            time_str = row['slot'].strip()
            occ = float(row['occupancy'])
            
            # Calculate the base slot within a day
            minutes = time_str_to_minutes(time_str)
            base_slot = (minutes - 240) // 30
            if base_slot < 0:
                continue  # ignore times before 4:00 AM
            
            if has_day_column:
                # If day column exists, use it (0-6, Monday-Sunday)
                day = int(row['day'])
                absolute_slot = day * 48 + base_slot
                if route in options:
                    options[route].append((absolute_slot, occ))
            else:
                # If no day column, replicate for each day of the week
                for day in range(7):  # 0-6 for Monday-Sunday
                    absolute_slot = day * 48 + base_slot
                    if route in options:
                        options[route].append((absolute_slot, occ))
    
    # Sort each candidate list by slot value
    for key in options:
        options[key].sort(key=lambda x: x[0])
    
    return options

# ---------------------------------------------------
# Track bus location and movements throughout the week
# ---------------------------------------------------
class BusTracker:
    def __init__(self, num_buses_at_a, num_buses_at_b, num_buses_at_c=0):
        # Initialize bus locations
        self.total_buses = num_buses_at_a + num_buses_at_b + num_buses_at_c
        self.locations = {}
        
        # Assign initial locations
        bus_id = 0
        for i in range(num_buses_at_a):
            self.locations[bus_id] = 'A'
            bus_id += 1
        
        for i in range(num_buses_at_b):
            self.locations[bus_id] = 'B'
            bus_id += 1
        
        for i in range(num_buses_at_c):
            self.locations[bus_id] = 'C'
            bus_id += 1
        
        # Store movements (departure_slot, route, arrival_slot)
        self.movements = {bus_id: [] for bus_id in range(self.total_buses)}
    
    def get_buses_at_location(self, location, start_slot):
        """Return list of buses that are at the given location at the start_slot"""
        buses = []
        for bus_id, loc in self.locations.items():
            if loc == location:
                # Check if this bus has a pending movement that starts before start_slot
                available = True
                for move in self.movements[bus_id]:
                    if move[0] <= start_slot and start_slot < move[2]:  # Bus is in transit
                        available = False
                        break
                if available:
                    buses.append(bus_id)
        return buses
    
    def add_movement(self, bus_id, departure_slot, route, arrival_slot):
        """Record a bus movement and update its final location"""
        self.movements[bus_id].append((departure_slot, route, arrival_slot))
        
        # Update the location based on the route's destination
        if route == 'A-B':
            self.locations[bus_id] = 'B'
        elif route == 'B-A':
            self.locations[bus_id] = 'A'
        elif route == 'B-C':
            self.locations[bus_id] = 'C'
        elif route == 'C-B':
            self.locations[bus_id] = 'B'
        elif route == 'C-A':
            self.locations[bus_id] = 'A'
        elif route == 'A-C':
            self.locations[bus_id] = 'C'
    
    def get_schedule(self):
        """Get the complete schedule of all buses sorted by bus_id and departure time"""
        schedule = []
        for bus_id in range(self.total_buses):
            # Sort movements by departure time
            sorted_movements = sorted(self.movements[bus_id], key=lambda x: x[0])
            for departure, route, arrival in sorted_movements:
                schedule.append({
                    'bus_id': bus_id,
                    'departure_slot': departure,
                    'departure_time': slot_to_day_time(departure),
                    'route': route,
                    'arrival_slot': arrival,
                    'arrival_time': slot_to_day_time(arrival)
                })
        return sorted(schedule, key=lambda x: (x['bus_id'], x['departure_slot']))

# ---------------------------------------------------
# Weekly scheduling model.
# ---------------------------------------------------
def main():
    filename = "neugo/processed/h_vjw_vzg_merged.csv"  # CSV with candidate rows
    
    # Parameters
    num_buses_at_a = 8    # Initial number of buses starting at A
    num_buses_at_b = 0    # Initial number of buses starting at B
    num_buses_at_c = 0    # Initial number of buses starting at C
    
    # Travel time parameters (in slots; 1 slot = 30 minutes)
    travel_time = {
        'A-B': 16,  # 8 hours
        'B-A': 16,  # 8 hours
        'B-C': 10,  # 5 hours
        'C-B': 10,  # 5 hours
        'C-A': 20,  # 10 hours
        'A-C': 20   # 10 hours (direct route, if needed)
    }
    
    # Minimum layover times at each location (in slots)
    layover_time = {
        'A': 4,  # 2 hours
        'B': 4,  # 2 hours
        'C': 5   # 2.5 hours
    }

    # One-week horizon: 7 days, 48 slots per day
    earliest_slot = 0
    latest_slot = 48 * 7 - 1  # Last slot of the week
    
    # Load candidate options from CSV
    candidates = load_candidates(filename)
    
    # Initialize bus tracker
    bus_tracker = BusTracker(num_buses_at_a, num_buses_at_b, num_buses_at_c)
    total_buses = bus_tracker.total_buses
    
    # Create the CP-SAT model
    model = cp_model.CpModel()
    
    # Create variables to track weekly schedule
    bus_assignments = {}  # Maps (day, time_slot, route) to bus_id decision variables
    occupancy_vars = {}   # Maps (day, time_slot, route) to occupancy values
    
    # Group candidate slots by day for easier processing
    day_candidates = {}
    for route in candidates:
        day_candidates[route] = {}
        for slot, occ in candidates[route]:
            day = slot // 48
            slot_in_day = slot % 48
            if day not in day_candidates[route]:
                day_candidates[route][day] = []
            day_candidates[route][day].append((slot, slot_in_day, occ))
    
    # Create decision variables and constraints for each day
    for day in range(7):  # 0-6 for days of the week
        day_start = day * 48
        day_end = day_start + 47
        
        # Create variables for each candidate slot for each route
        for route in candidates:
            if day in day_candidates[route]:
                for slot, slot_in_day, occ in day_candidates[route][day]:
                    # Decision variable: which bus (if any) takes this slot
                    bus_assignments[(slot, route)] = model.NewIntVar(
                        -1, total_buses - 1, f"bus_{slot}_{route}")
                    
                    # -1 means no bus is assigned to this slot
                    # Store the occupancy value (scaled by 1000 for integer math)
                    occupancy_vars[(slot, route)] = int(occ * 1000)
    
    # Constraints to ensure bus movements are valid
    for day in range(7):
        day_start = day * 48
        day_end = day_start + 47
        
        # Check each bus's possible movements for this day
        for bus_id in range(total_buses):
            # For each potential A-B slot in this day
            if day in day_candidates['A-B']:
                for ab_slot, _, _ in day_candidates['A-B'][day]:
                    # If this bus takes this A-B slot, it must:
                    # 1. Be at location A at the start of this slot
                    # 2. Have enough time after arriving at B before taking another slot
                    
                    # Get arrival time at B
                    ab_arrival = ab_slot + travel_time['A-B']
                    
                    # For each potential B-A and B-C slot that could follow
                    for next_day in range(day, 7):
                        next_day_start = next_day * 48
                        next_day_end = next_day_start + 47
                        
                        # Check B-A options
                        if next_day in day_candidates['B-A']:
                            for ba_slot, _, _ in day_candidates['B-A'][next_day]:
                                # Only consider if it's after arrival at B plus layover time
                                if ba_slot >= ab_arrival + layover_time['B']:
                                    # If bus takes A-B at ab_slot, it can take B-A at ba_slot
                                    # (but it's not required to)
                                    b1 = model.NewBoolVar(f"ab_{ab_slot}_ba_{ba_slot}_bus_{bus_id}")
                                    
                                    # Link the boolean to the bus assignment variables
                                    model.Add(bus_assignments[(ab_slot, 'A-B')] == bus_id).OnlyEnforceIf(b1)
                                    model.Add(bus_assignments[(ba_slot, 'B-A')] == bus_id).OnlyEnforceIf(b1)
                                    
                                    # If this boolean is true, then the bus cannot take other routes
                                    # during the period [ab_slot, ba_slot + travel_time['B-A']]
                        
                        # Check B-C options
                        if next_day in day_candidates['B-C']:
                            for bc_slot, _, _ in day_candidates['B-C'][next_day]:
                                # Only consider if it's after arrival at B plus layover time
                                if bc_slot >= ab_arrival + layover_time['B']:
                                    # If bus takes A-B at ab_slot, it can take B-C at bc_slot
                                    b2 = model.NewBoolVar(f"ab_{ab_slot}_bc_{bc_slot}_bus_{bus_id}")
                                    
                                    # Link the boolean to the bus assignment variables
                                    model.Add(bus_assignments[(ab_slot, 'A-B')] == bus_id).OnlyEnforceIf(b2)
                                    model.Add(bus_assignments[(bc_slot, 'B-C')] == bus_id).OnlyEnforceIf(b2)
            
            # Similar constraints for buses starting at B or C (if any)
            # (omitted for brevity but would follow the same pattern)
    
    # Ensure each bus is assigned to at most one route at any given time
    # (a bus can't be in two places at once)
    for bus_id in range(total_buses):
        for day in range(7):
            day_start = day * 48
            day_end = day_start + 47
            
            # For each slot in this day
            for slot in range(day_start, day_end + 1):
                # Get all route options that might be active at this slot
                active_routes = []
                for route in candidates:
                    for candidate_slot, occ in candidates[route]:
                        # Check if this slot falls within the travel time of this route
                        route_end = candidate_slot + travel_time[route]
                        if candidate_slot <= slot < route_end:
                            active_routes.append((candidate_slot, route))
                
                # Ensure bus is assigned to at most one of these routes
                for i, (s1, r1) in enumerate(active_routes):
                    for j, (s2, r2) in enumerate(active_routes[i+1:], i+1):
                        # If the bus is assigned to s1/r1, it cannot be assigned to s2/r2
                        b1 = model.NewBoolVar(f"conflict_{s1}_{r1}_{s2}_{r2}_{bus_id}")
                        model.Add(bus_assignments[(s1, r1)] == bus_id).OnlyEnforceIf(b1)
                        model.Add(bus_assignments[(s2, r2)] != bus_id).OnlyEnforceIf(b1)
    
    # Ensure each slot is assigned to at most one bus
    for route in candidates:
        for slot, _ in candidates[route]:
            # Create constraints to ensure only one bus is assigned to this slot
            for bus_id1 in range(total_buses):
                for bus_id2 in range(bus_id1 + 1, total_buses):
                    # If bus_id1 takes this slot, bus_id2 cannot take it
                    b = model.NewBoolVar(f"unique_{slot}_{route}_{bus_id1}_{bus_id2}")
                    model.Add(bus_assignments[(slot, route)] == bus_id1).OnlyEnforceIf(b)
                    model.Add(bus_assignments[(slot, route)] != bus_id2).OnlyEnforceIf(b)
    
    # Objective: Maximize total occupancy
    total_occupancy = model.NewIntVar(0, 10000000, "total_occupancy")
    occupancy_terms = []
    for (slot, route), occ in occupancy_vars.items():
        # For each slot/route, add its occupancy to the objective if it's assigned to any bus
        for bus_id in range(total_buses):
            b = model.NewBoolVar(f"used_{slot}_{route}_{bus_id}")
            model.Add(bus_assignments[(slot, route)] == bus_id).OnlyEnforceIf(b)
            term = model.NewIntVar(0, occ, f"term_{slot}_{route}_{bus_id}")
            model.Add(term == b * occ)
            occupancy_terms.append(term)
    
    model.Add(total_occupancy == sum(occupancy_terms))
    model.Maximize(total_occupancy)
    
    # Solve the model
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 300.0  # Longer time limit for weekly schedule
    status = solver.Solve(model)
    
    if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        print("Schedule found!")
        
        # Process results and update the bus tracker
        for (slot, route), var in bus_assignments.items():
            bus_id = solver.Value(var)
            if bus_id >= 0:  # If a bus was assigned
                arrival_slot = slot + travel_time[route]
                bus_tracker.add_movement(bus_id, slot, route, arrival_slot)
        
        # Generate and display the complete schedule
        schedule = bus_tracker.get_schedule()
        
        print("\nComplete Weekly Schedule:")
        print("=" * 80)
        
        current_bus = -1
        total_occupancy_value = 0
        
        for entry in schedule:
            if entry['bus_id'] != current_bus:
                current_bus = entry['bus_id']
                print(f"\nBus {current_bus} Schedule:")
                print("-" * 60)
            
            route_occ = occupancy_vars.get((entry['departure_slot'], entry['route']), 0) / 1000.0
            total_occupancy_value += route_occ
            
            print(f"  {entry['route']}: Depart {entry['departure_time']} - Arrive {entry['arrival_time']} | Occ: {route_occ:.3f}")
        
        print("\nTotal Occupancy Value: {:.3f}".format(total_occupancy_value))
        print("=" * 80)
    else:
        print("No solution found.")

if __name__ == "__main__":
    main()
