import { Request, Response } from 'express';
import moment from 'moment';
import httpStatus from 'http-status';
import { IResponse, Result } from '../types/resposne';
import _ from 'lodash';
import { AmlError } from '../types/AmlError';

const ResponseHandler = {
  successResponse: (req: Request, res: Response, result: Result) => {
    const { body } = req as any;
    const msgid = _.get(body, ['params', 'msgid']);
    const resmsgid = _.get(res, 'resmsgid');
    res.status(result.status || 200).json(ResponseHandler.refactorResponse({ id: (req as any).id, result: result.data, msgid, resmsgid }));
  },

  refactorResponse: ({ id = 'api', ver = '1.0', params = { status: 'SUCCESS' }, responseCode = httpStatus['200_NAME'], result = {}, msgid = '', resmsgid = '' }): IResponse => {
    const paramsObj = { ...params, ...(!_.isEmpty(msgid) && { msgid }), resmsgid };
    return <IResponse>{ id, ver, ts: moment().format(), params: paramsObj, responseCode, result };
  },

  amlErrorResponse: (error: AmlError, req: Request, res: Response) => {
    const { statusCode, message, errCode, code = 'INTERNAL_SERVER_ERROR', data } = error;
    const { id, body } = req as any;
    const msgid = _.get(body, ['params', 'msgid']);
    const resmsgid = _.get(res, 'resmsgid');
    const response = ResponseHandler.refactorResponse({ id, msgid, params: { status: 'FAILED' }, responseCode: errCode || httpStatus['500_NAME'], resmsgid, result: data });
    res.status(statusCode || httpStatus.INTERNAL_SERVER_ERROR).json({ ...response, error: { code, message } });
  },
};

export { ResponseHandler };
