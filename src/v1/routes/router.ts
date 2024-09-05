import express from 'express';
import { setDataToRequestObject } from '../middleware/setDataToReqObj';
import getTemplate from '../controllers/template/getTemplate';
import uploadTemplate from '../controllers/template/uploadtemplate';
import uploadQuestion from '../controllers/BulkUpload/questionUpload';
import uploadStatus from '../controllers/BulkUpload/uploadStatus';
import { manualTriggerQuestion } from '../controllers/BulkUpload/questionStatusJob';

export const router = express.Router();

//get signed url
router.post('/get/template', setDataToRequestObject('api.get.template'), getTemplate);

//upload  url for template
router.post('/upload/template', setDataToRequestObject('api.upload.template'), uploadTemplate);

//upload url for question
router.post('/upload/question', setDataToRequestObject('api.upload.question'), uploadQuestion);

//status for the uplaoded data
router.get('/upload/status/:process_id', setDataToRequestObject('api.upload.status'), uploadStatus);

router.get('/manual/trigger/questionUpload', setDataToRequestObject('api.manual.trigger'), manualTriggerQuestion);
