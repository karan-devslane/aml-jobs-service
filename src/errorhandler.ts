import { NextFunction, Request, Response } from 'express';
import { ResponseHandler } from './utils/responseHandler';
import _ from 'lodash';
import { AmlError } from './types/AmlError';
import logger from './utils/logger';

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export const amlErrorHandler = (amlErr: AmlError, req: Request, res: Response, _next: NextFunction) => {
  logger.error({ apiId: _.get(req, 'id'), resmsgid: _.get(res, 'resmsgid'), ...amlErr });
  ResponseHandler.amlErrorResponse(amlErr, req, res);
};
