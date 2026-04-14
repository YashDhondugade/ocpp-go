package logging

// Event represents a category of log events that can be selectively enabled or disabled.
type Event string

const (
	EventConnect     Event = "connect"      // client connect
	EventDisconnect  Event = "disconnect"   // client disconnect
	EventMessageIn   Event = "message_in"   // incoming CALL / CALLRESULT / CALLERROR
	EventMessageOut  Event = "message_out"  // enqueued / sent CALL / CALLRESULT / CALLERROR
	EventDispatch    Event = "dispatch"     // OCPP handler invocation
	EventWS          Event = "ws"           // ws read / write / ping
	EventServerState Event = "server_state" // Start / Stop
	EventServerHealth Event = "server_health" // capacity, backpressure, stalls
)
