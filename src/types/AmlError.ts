export class AmlError {
  code: string;
  message: string;
  errCode: string;
  statusCode: number;
  data: undefined;
  err: Error | undefined;

  constructor(code: string, message: string, errorCode: string, statusCode: number, data?: any, err?: Error) {
    this.code = code;
    this.message = message;
    this.errCode = errorCode;
    this.statusCode = statusCode;
    this.data = data;
    this.err = err;
  }
}

export const amlError = (code: string, message: string, errorCode: string, statusCode: number, data?: any, err?: Error) => {
  return new AmlError(code, message, errorCode, statusCode, data, err);
};
