import { QuestionSetStage } from '../models/questionSetStage';
import { Optional } from 'sequelize';
import logger from '../utils/logger';

export const createQuestionSetStage = async (req: Optional<any, any>[]): Promise<any> => {
  try {
    const stagingData = await QuestionSetStage.bulkCreate(req);
    const [dataValues] = stagingData;
    return { dataValues };
  } catch (error) {
    logger.error(error);
  }
};

//get Single QuestionSet by meta data
export const questionSetStageMetaData = async (req: any): Promise<any> => {
  try {
    const questionSets = await QuestionSetStage.findAll({ where: req });
    return { questionSets };
  } catch (error) {
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to get a record' : '';
    return { error: true, message: errorMsg };
  }
};

//update single QuestionSet
export const updateQuestionStageSet = async (whereClause: any, req: any): Promise<any> => {
  try {
    const updateQuestionSet = await QuestionSetStage.update(req, { where: whereClause });

    return { error: false, updateQuestionSet };
  } catch (error) {
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to update a record' : '';
    return { error: true, message: errorMsg };
  }
};

//get Single QuestionSet by id
export const questionSetStageById = async (id: number): Promise<any> => {
  try {
    const getQuestionSet = await QuestionSetStage.findOne({ where: { id } });
    return { error: false, getQuestionSet };
  } catch (error) {
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to get a record' : '';
    return { error: true, message: errorMsg };
  }
};
