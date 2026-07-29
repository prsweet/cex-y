type responseType = {
  (success: false, data: null, error: object): any;
  (success: true, data: object, error: null): any;
};

export const response: responseType = (success: boolean, data: object | null, error: object | null) => {
  return {
    success: success,
    data: data,
    error: error
  };
};

export const errors = {
  typebox400: "INVALID_REQUEST",
  emailConflict409: "EMAIL_ALREADY_EXISTS",
  internalServer500: "INTERNAL_SERVER_ERROR",
  userNotFound404: "USER_NOT_FOUND",
  invalidCredentials401: "INVALID_CREDENTIALS",
  invalidToken401: "INVALID_TOKEN",
  unauthorized401: "UNAUTHORIZED"
};

/*
  the unknown errors will have error field having the real error
  otherwise all will have just message fields
*/