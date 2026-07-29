import { engineController } from "../controllers/engineController";
import { errors, response } from "../schemas/responses";
import { baseApp, type decoratedContext } from "../middlewares/base";
import { status } from "elysia";
import { cancelOrderSchema, placeOrderSchema } from "../schemas/engineSchema";

export const engineRoute = baseApp.group('/engine', (app) =>
  app
    .onBeforeHandle(({ userId }: decoratedContext) => {
      if (!userId) return status(401, response(false, null, { message: errors.unauthorized401 }));
    })
    .post('/create-order', engineController.createOrder, { body: placeOrderSchema })
    .delete('/cancel-order', engineController.cancelOrder, { body: cancelOrderSchema })
)