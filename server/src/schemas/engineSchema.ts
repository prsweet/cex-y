import { t } from "elysia";

export const placeOrderSchema = t.Object({
  symbol: t.String(),
  side: t.UnionEnum(["Buy", "Sell"]),
  order_type: t.UnionEnum(["Limit", "Market"]),
  price: t.Number({ minimum: 1 }),
  quantity: t.Number({ minimum: 1 })
});

export const cancelOrderSchema = t.Object({
  symbol: t.String(),
  orderId: t.String()
});