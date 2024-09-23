import { QuestionStage } from '../models/questionStage';
import logger from '../utils/logger';

export const createQuestionStage = async (insertData: Record<string, unknown>[]): Promise<any> => {
  try {
    const stagingData = await QuestionStage.bulkCreate(insertData);
    const [dataValues] = stagingData;
    return { dataValues };
  } catch (error) {
    logger.error(error);
  }
};

export const questionStageMetaData = async (whereClause: any): Promise<any> => {
  try {
    const Questions = await QuestionStage.findAll({ where: whereClause });
    const questions = Questions.map((q) => q.dataValues);
    return questions;
  } catch (error) {
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to get a record' : '';
    return { error: true, message: errorMsg };
  }
};

export const updateQuestionStage = async (whereClause: any, updateObj: any): Promise<any> => {
  try {
    const updateQuestionStage = await QuestionStage.update(updateObj, { where: whereClause });

    return { error: false, updateQuestionStage };
  } catch (error) {
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to update a record' : '';
    return { error: true, message: errorMsg };
  }
};

export const getAllStageQuestion = async (): Promise<any> => {
  try {
    const Questions = await QuestionStage.findAll({});
    const questions = Questions.map((q) => q.dataValues);
    return questions;
  } catch (error) {
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to get a record' : '';
    return { error: true, message: errorMsg };
  }
};
