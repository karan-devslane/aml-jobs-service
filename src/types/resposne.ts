export interface Params {
  status: string;
  resmsgid: string;
  msgid: string;
}

export interface IResponse {
  id: string;
  ts: string;
  ver: string;
  params: Params;
  responseCode: string;
  result: any;
}

export interface Result {
  data: object;
  status: number;
}
