package events

type EventAction int

const (
	Add EventAction = iota
	Remove
)

type BlockedProduct struct {
	ProductId int
	Action    EventAction
}
