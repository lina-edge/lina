package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/lightningnetwork/lnd/lnrpc"
	lightningmodel "github.com/robertodantas/lnpay/proto/gen/model/lightning"
)

type LNDEventStream struct {
	lndClient   *LNDClient
	subscribers []chan *lightningmodel.LightningEvent
	mu          sync.RWMutex
}

func NewLNDEventStream(lndClient *LNDClient) *LNDEventStream {
	return &LNDEventStream{
		lndClient:   lndClient,
		subscribers: make([]chan *lightningmodel.LightningEvent, 0),
	}
}

// Subscribe adds a new subscriber to receive events.
func (es *LNDEventStream) Subscribe() <-chan *lightningmodel.LightningEvent {
	es.mu.Lock()
	defer es.mu.Unlock()

	ch := make(chan *lightningmodel.LightningEvent, 100)
	es.subscribers = append(es.subscribers, ch)
	return ch
}

// Unsubscribe removes a subscriber.
func (es *LNDEventStream) Unsubscribe(ch <-chan *lightningmodel.LightningEvent) {
	es.mu.Lock()
	defer es.mu.Unlock()

	for i, sub := range es.subscribers {
		if sub == ch {
			close(sub)
			es.subscribers = append(es.subscribers[:i], es.subscribers[i+1:]...)
			break
		}
	}
}

// Publish sends an event to all subscribers.
func (es *LNDEventStream) Publish(event *lightningmodel.LightningEvent) {
	es.mu.RLock()
	defer es.mu.RUnlock()

	for _, ch := range es.subscribers {
		select {
		case ch <- event:
		default:
			log.Printf("subscriber channel full, dropping lightning event")
		}
	}
}

// Start begins listening for LND invoice updates and publishing events.
func (es *LNDEventStream) Start(ctx context.Context) error {
	stream, err := es.lndClient.SubscribeInvoices(ctx, 0, 0)
	if err != nil {
		return fmt.Errorf("failed to subscribe to invoices: %w", err)
	}

	log.Println("LND event stream started, listening for invoice updates...")

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Println("LND event stream stopped")
				return
			default:
				invoice, err := stream.Recv()
				if err != nil {
					log.Printf("error receiving invoice update: %v", err)
					time.Sleep(5 * time.Second)
					stream, err = es.lndClient.SubscribeInvoices(ctx, 0, 0)
					if err != nil {
						log.Printf("failed to reconnect invoice stream: %v", err)
					}
					continue
				}

				if event := es.buildEventFromInvoice(invoice); event != nil {
					es.Publish(event)
				}
			}
		}
	}()

	return nil
}

func (es *LNDEventStream) buildEventFromInvoice(invoice *lnrpc.Invoice) *lightningmodel.LightningEvent {
	deviceMeta := decodeInvoiceMetadata(invoice.Memo)
	invoiceID := fmt.Sprintf("%x", invoice.RHash)
	amountMsat := invoice.ValueMsat
	if amountMsat == 0 {
		amountMsat = invoice.Value * 1000
	}

	expiresAt := time.Unix(invoice.CreationDate+invoice.Expiry, 0).UTC().Format(time.RFC3339)
	stateName := invoice.State.String()
	log.Printf("Processing invoice update: invoice_id=%s state=%s device_id=%s amount_msat=%d", invoiceID, stateName, deviceMeta.DeviceID, amountMsat)

	switch invoice.State {
	case lnrpc.Invoice_OPEN, lnrpc.Invoice_ACCEPTED:
		lnInvoice := &lightningmodel.Invoice{
			InvoiceId:  invoiceID,
			DeviceId:   deviceMeta.DeviceID,
			Bolt11:     invoice.PaymentRequest,
			AmountMsat: amountMsat,
			Status:     mapInvoiceStatus(invoice.State),
			ExpiresAt:  expiresAt,
		}
		return &lightningmodel.LightningEvent{
			Type: lightningmodel.LightningEventType_LIGHTNING_EVENT_TYPE_INVOICE_CREATED,
			Payload: &lightningmodel.LightningEvent_InvoiceCreated{
				InvoiceCreated: &lightningmodel.InvoiceCreatedEvent{
					Invoice: lnInvoice,
				},
			},
		}
	case lnrpc.Invoice_SETTLED:
		timestamp := time.Unix(invoice.SettleDate, 0).UTC().Format(time.RFC3339)
		log.Printf("Invoice settled: invoice_id=%s amount_received_msat=%d device_id=%s", invoiceID, invoice.AmtPaidSat*1000, deviceMeta.DeviceID)
		return &lightningmodel.LightningEvent{
			Type: lightningmodel.LightningEventType_LIGHTNING_EVENT_TYPE_INVOICE_SETTLED,
			Payload: &lightningmodel.LightningEvent_InvoiceSettled{
				InvoiceSettled: &lightningmodel.InvoiceSettledEvent{
					InvoiceId:          invoiceID,
					DeviceId:           deviceMeta.DeviceID,
					AmountReceivedMsat: invoice.AmtPaidSat * 1000,
					NewBalanceMsat:     0,
					Timestamp:          timestamp,
				},
			},
		}
	case lnrpc.Invoice_CANCELED:
		timestamp := time.Unix(invoice.CreationDate+invoice.Expiry, 0).UTC().Format(time.RFC3339)
		log.Printf("Invoice expired/canceled: invoice_id=%s device_id=%s", invoiceID, deviceMeta.DeviceID)
		return &lightningmodel.LightningEvent{
			Type: lightningmodel.LightningEventType_LIGHTNING_EVENT_TYPE_INVOICE_EXPIRED,
			Payload: &lightningmodel.LightningEvent_InvoiceExpired{
				InvoiceExpired: &lightningmodel.InvoiceExpiredEvent{
					InvoiceId: invoiceID,
					DeviceId:  deviceMeta.DeviceID,
					Timestamp: timestamp,
				},
			},
		}
	default:
		log.Printf("Ignoring invoice update with unsupported state: invoice_id=%s state=%s", invoiceID, stateName)
		return nil
	}
}

func mapInvoiceStatus(state lnrpc.Invoice_InvoiceState) lightningmodel.InvoiceStatus {
	switch state {
	case lnrpc.Invoice_SETTLED:
		return lightningmodel.InvoiceStatus_INVOICE_STATUS_SETTLED
	case lnrpc.Invoice_CANCELED:
		return lightningmodel.InvoiceStatus_INVOICE_STATUS_EXPIRED
	default:
		return lightningmodel.InvoiceStatus_INVOICE_STATUS_CREATED
	}
}
