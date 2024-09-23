import { QuestionSetStage } from '../models/questionSetStage';
import logger from '../utils/logger';

export const createQuestionSetStage = async (insertData: Array<Record<string, any>>): Promise<any> => {
  try {
    const stagingData = await QuestionSetStage.bulkCreate(insertData);
    const [dataValues] = stagingData;
    return { dataValues };
  } catch (error) {
    logger.error(error);
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to create record' : '';
    return { error: true, message: errorMsg };
  }
};

export const questionSetStageMetaData = async (whereClause: any): Promise<any> => {
  try {
    const QuestionSets = await QuestionSetStage.findAll({ where: whereClause });
    const questionSets = QuestionSets.map((qs) => qs.dataValues);
    return questionSets;
  } catch (error) {
    logger.error(error);
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to get all record' : '';
    return { error: true, message: errorMsg };
  }
};

export const updateQuestionStageSet = async (whereClause: any, updateObj: any): Promise<any> => {
  try {
    const updateQuestionSet = await QuestionSetStage.update(updateObj, { where: whereClause });
    return { error: false, updateQuestionSet };
  } catch (error) {
    logger.error(error);
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to update a record' : '';
    return { error: true, message: errorMsg };
  }
};

export const getAllStageQuestionSet = async (): Promise<any> => {
  try {
    const QuestionSets = await QuestionSetStage.findAll({});
    const questionSets = QuestionSets.map((qs) => qs.dataValues);
    return questionSets;
  } catch (error) {
    logger.error(error);
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to get all record' : '';
    return { error: true, message: errorMsg };
  }
};
