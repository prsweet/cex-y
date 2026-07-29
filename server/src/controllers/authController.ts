import { status, type Context, type Static } from "elysia"
import type { loginSchema, signupSchema } from "../schemas/authSchema"
import { prisma } from "../db"
import { errors, response } from "../schemas/responses";
import type { decoratedContext } from "../middlewares/base";

const signupUser = async ({ body, set }: decoratedContext<{ body: Static<typeof signupSchema> }>) => {
  try {
    const userExist = await prisma.user.findUnique({ where: { email: body.email } });
    if (userExist) {
      set.status = 409;
      throw new Error(response(false, null, {
        error: userExist,
        message: errors.emailConflict409
      }));
    }
    
    let hasedPassword = await Bun.password.hash(body.password, { algorithm: "bcrypt" });
    let createdUser = await prisma.user.create({
      data: {
        name: body.name,
        email: body.email,
        password: hasedPassword
      },
      omit: { createdAt: true, password: true }
    });
    return status(200, response(true, createdUser, null));
  } catch (e) {
    return status(500, response(false, null, {
      error: e,
      message: errors.internalServer500
    }));
  }
}

const loginUser = async ({ body, set, jwt }: decoratedContext<{ body: Static<typeof loginSchema> }> & {
  jwt: { sign(payload: Record<string, any>): Promise<string> }
}) => {
  try {
    const userExist = await prisma.user.findFirst({ where: { email: body.email } });
    if (!userExist) {
      set.status = 404;
      throw new Error(response(false, null, {
        error: userExist,
        message: errors.userNotFound404
      }));
    }
  
    const isMatch = await Bun.password.verify(body.password, userExist.password);
    if (!isMatch) {
      set.status = 401;
      throw new Error(response(false, null, {
        error: isMatch,
        message: errors.invalidCredentials401
      }));
    }
  
    const token = await jwt.sign({ userId: userExist.id });
    return status(200, response(true, { token }, null));
  } catch (e) {
    return status(500, response(false, null, {
      error: e,
      message: errors.internalServer500
    }));
  }
}

export const authControllers = {
  signupUser,
  loginUser
}