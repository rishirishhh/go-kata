package concurrent_aggregator

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/medunes/go-kata/01-context-cancellation-concurrency/01-concurrent-aggregator/order"
	"github.com/medunes/go-kata/01-context-cancellation-concurrency/01-concurrent-aggregator/profile"
)

// ------------------------------------
// Profile service implementation
// ------------------------------------

type ProfileClient struct{}

func (p *ProfileClient) Get(ctx context.Context, id int) (*profile.Profile, error) {
	// Simulate network/database work.
	select {
	case <-time.After(500 * time.Millisecond):
		return &profile.Profile{
			Name: "Rishi",
		}, nil

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ------------------------------------
// Order service implementation
// ------------------------------------

type OrderClient struct{}

func (o *OrderClient) GetAll(ctx context.Context, id int) ([]*order.Order, error) {
	// Simulate network/database work.
	select {
	case <-time.After(700 * time.Millisecond):
		return []*order.Order{
			{
				UserId: id,
				Cost:   1200,
			},
			{
				UserId: id,
				Cost:   500,
			},
			{
				UserId: 999,
				Cost:   4000,
			},
		}, nil

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func main() {
	logger := slog.New(
		slog.NewTextHandler(os.Stdout, nil),
	)

	profileClient := &ProfileClient{}
	orderClient := &OrderClient{}

	userAggregator := NewUserAggregator(
		orderClient,
		profileClient,

		WithLogger(logger),
		WithTimeout(2*time.Second),
	)

	ctx := context.Background()

	result, err := userAggregator.Aggregate(ctx, 1)
	if err != nil {
		logger.Error(
			"aggregation failed",
			"err", err,
		)
		return
	}

	for _, item := range result {
		fmt.Printf(
			"Name: %s | Cost: %.2f\n",
			item.Name,
			item.Cost,
		)
	}
}
