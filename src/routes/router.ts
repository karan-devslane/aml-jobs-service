import express from 'express';
import { setDataToRequestObject } from '../middleware/setDataToReqObj';
import { manualTriggerQuestion } from '../controllers/BulkUpload/questionStatusJob';

export const router = express.Router();

//manual trigger the job
router.get('/manual/trigger/questionUpload', setDataToRequestObject('api.manual.trigger'), manualTriggerQuestion);
