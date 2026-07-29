import { status, type Static } from "elysia";
import type { decoratedContext } from "../middlewares/base";
import { cancelOrderSchema, placeOrderSchema } from "../schemas/engineSchema";
import { producer, redis } from "../schemas/clients";
import { errors, response } from "../schemas/responses";

const createOrder = async ({ body, userId }: decoratedContext<{ body: Static<typeof placeOrderSchema> }>) => {
  try {
    const orderCommand = {
      PlaceOrder: {
        symbol: body.symbol,
        user_id: userId,
        side: body.side,
        order_type: body.order_type,
        price: body.price,
        quantity: body.quantity
      }
    };

    await producer.send({
      topic: "orders",
      messages: [{
        key: body.symbol,
        value: JSON.stringify(orderCommand)
      }]
    });
    
    return status(200, response(true, orderCommand, null));
    // i will receive the order_id which i can give to user when cancelling
    // still have to write some process for receiving the order details after matching
    
  } catch (e) {
    console.log(e, "create-order");
    return status(500, response(false, null, {
      error: e,
      message: errors.internalServer500
    }));
  }
}

const cancelOrder = async ({ body }: decoratedContext<{ body: Static<typeof cancelOrderSchema> }>) => {
  try {
    const cancelCommand = {
      symbol: body.symbol,
      order_id: body.orderId
    };

    await producer.send({
      topic: "orders",
      messages: [{
        key: body.symbol,
        value: body.orderId
      }]
    });

    return status(200, response(true, cancelCommand, null));
  } catch (e) {
    console.log(e, "cancel-order");
    return status(500, response(false, null, {
      error: e,
      message: errors.internalServer500
    }));
  }
}

export const engineController = {
  createOrder,
  cancelOrder
}