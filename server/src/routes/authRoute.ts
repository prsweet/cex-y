import Elysia from "elysia";
import { authControllers } from "../controllers/authController";
import { loginSchema, signupSchema } from "../schemas/authSchema";
import { baseApp } from "../middlewares/base";

export const authRoute = baseApp.group('/auth', (app) =>
  app
    .post('/signup', authControllers.signupUser, {body: signupSchema})
    .post('/login', authControllers.loginUser, {body: loginSchema})
)