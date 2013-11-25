from Trip import *
from LocationGenerator import *

def main():
    numTrips = 6
    trips = []
    for x in range(0, numTrips):
        trip = Trip()
        trips.append(trip)
        trip.startTrip()
        # at this point the Trip is creating new points every second.

#    trip1.startTrip()
#    trip1.join()

#    trip2.startTrip(1,2)

#     points = LocationGenerator()
#     print points.getNext()
#     print points.getNext()
#     print points.getNext()

if __name__ == "__main__":
    main()
