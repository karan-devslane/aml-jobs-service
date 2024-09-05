import { tempCSVData } from '../models/tempCsvData';
import { AppDataSource } from '../config';
import { Optional } from 'sequelize';

//create service for tempCSVData
export const createtempCSVData = async (req: Optional<any, string>): Promise<any> => {
  const transact = await AppDataSource.transaction();
  try {
    await tempCSVData.create(req, { transaction: transact });
    await transact.commit();
    return { error: false, message: 'success' };
  } catch (error) {
    await transact.rollback();
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to create a record' : '';
    return { error: true, message: errorMsg };
  }
};

//get Single tempCSVData by meta data
export const gettempCSVDataByMetaData = async (req: any): Promise<any> => {
  try {
    req.is_active = true;
    const gettempCSVData = await tempCSVData.findAll({ where: req, raw: true });
    return { error: false, gettempCSVData };
  } catch (error) {
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to get a record' : '';
    return { error: true, message: errorMsg };
  }
};

//update single tempCSVData
export const updatetempCSVData = async (whereClause: any, req: any): Promise<any> => {
  try {
    const transact = await AppDataSource.transaction();
    whereClause.is_active = true;
    const updatetempCSVData = await tempCSVData.update(req, { where: whereClause, transaction: transact });
    await transact.commit();
    return { error: false, updatetempCSVData };
  } catch (error) {
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to update a record' : '';
    return { error: true, message: errorMsg };
  }
};

//get Single tempCSVData by id
export const gettempCSVDataById = async (tempCSVData_id: string): Promise<any> => {
  try {
    const whereClause: Record<string, any> = { tempCSVData_id };
    whereClause.is_active = true;
    const gettempCSVData = await tempCSVData.findOne({ where: whereClause, raw: true });
    return { error: false, gettempCSVData };
  } catch (error) {
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to get a record' : '';
    return { error: true, message: errorMsg };
  }
};
