import Elysia, { type RouteSchema } from "elysia";
import { authPlugin } from "./auth";
import type { Context } from "elysia";

export type decoratedContext<T extends RouteSchema = RouteSchema> = Context<T> & {
  userId?: string,
}

export const baseApp = new Elysia()
  .use(authPlugin)