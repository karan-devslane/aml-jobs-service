/* eslint-disable @typescript-eslint/no-unused-vars */
import { Request, Response } from 'express';
import httpStatus from 'http-status';
import _ from 'lodash';
import { AmlError } from '../types/AmlError';

const ResponseHandler = {
  amlErrorResponse: (error: AmlError, req: Request, res: Response) => {},
};

export { ResponseHandler };
