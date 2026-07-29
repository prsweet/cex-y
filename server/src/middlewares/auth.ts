import jwt from "@elysiajs/jwt";
import Elysia, { status } from "elysia";
import { errors, response } from "../schemas/responses";

export type jwtPayload = {
  userId: string,
}

export const authPlugin = new Elysia({ name: "auth" })
  .use(jwt({
    name: "jwt",
    secret: process.env.JWT_SECRET!,
    exp: '14d'
  }))
  .derive({as: "global"}, async ({ headers, jwt }) => {
    try {
      const token = headers.authorization?.split(" ")[1];
      const decoded = await jwt.verify(token) as jwtPayload;
      let userId = decoded.userId;
      return { userId };
    } catch (e) {
      console.log(e, "authPlugin");
      return status(401, response(false, null, {
        message: errors.invalidToken401
      }))
    }
  })