import { Elysia, status, type Context, type RouteSchema } from "elysia";
import { authRoute } from "./routes/authRoute";
import { engineRoute } from "./routes/engineRoute";
import { errors, response } from "./schemas/responses";
import { authPlugin } from "./middlewares/auth";
import { producer } from "./schemas/clients";

await producer.connect();

new Elysia()
  .onError(({ code, error }) => {
    if (code == 'VALIDATION') return status(400, response(false, null, {
      error: error,
      message: errors.typebox400
    }));
  })
  .use(authPlugin)
  .use(authRoute)
  .use(engineRoute)
  .listen(3000, () => console.log("server is runnign on 3000"));