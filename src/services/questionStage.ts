import { QuestionStage } from '../models/questionStage';
import { Optional } from 'sequelize';

export const createQuestionStage = async (req: Optional<any, any>[]): Promise<any> => {
  try {
    const stagingData = await QuestionStage.bulkCreate(req);
    const [dataValues] = stagingData;
    return { dataValues };
  } catch (error) {
    logger.error(error);
  }
};

//get Single Question by meta data
export const questionStageMetaData = async (req: any): Promise<any> => {
  try {
    const questions = await QuestionStage.findAll({ where: req });
    return { questions };
  } catch (error) {
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to get a record' : '';
    return { error: true, message: errorMsg };
  }
};

//update single Question
export const updateQuestionStage = async (whereClause: any, req: any): Promise<any> => {
  try {
    const updateQuestionStage = await QuestionStage.update(req, { where: whereClause });

    return { error: false, updateQuestionStage };
  } catch (error) {
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to update a record' : '';
    return { error: true, message: errorMsg };
  }
};

//get Single Question by id
export const questionStageById = async (id: number): Promise<any> => {
  try {
    const getQuestion = await QuestionStage.findOne({ where: { id } });
    return { error: false, getQuestion };
  } catch (error) {
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to get a record' : '';
    return { error: true, message: errorMsg };
  }
};
