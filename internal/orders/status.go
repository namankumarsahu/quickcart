package orders

// Status is an order's lifecycle state.
type Status string

const (
	StatusCreated   Status = "created"
	StatusReserved  Status = "reserved"
	StatusConfirmed Status = "confirmed"
	StatusPacked    Status = "packed"
	StatusDelivered Status = "delivered"
	StatusCancelled Status = "cancelled"
	StatusExpired   Status = "expired"
)

// transitions is a read-only lookup table: it is filled once at program start
// and never modified. Terminal states are present with an empty set, so
// Valid can tell "terminal" apart from "not a status at all".
// The database trigger in migration 002 must list exactly the same pairs.
var transitions = map[Status]map[Status]bool{
	StatusCreated:   {StatusReserved: true, StatusCancelled: true},
	StatusReserved:  {StatusConfirmed: true, StatusCancelled: true, StatusExpired: true},
	StatusConfirmed: {StatusPacked: true, StatusCancelled: true},
	StatusPacked:    {StatusDelivered: true},
	StatusDelivered: {},
	StatusCancelled: {},
	StatusExpired:   {},
}

// Valid reports whether s is one of the known statuses.
func (s Status) Valid() bool {
	_, ok := transitions[s]
	return ok
}

// CanTransition reports whether an order may move from -> to.
// Looking up a missing key in a nil inner map is safe in Go and yields false.
func CanTransition(from, to Status) bool {
	return transitions[from][to]
}
