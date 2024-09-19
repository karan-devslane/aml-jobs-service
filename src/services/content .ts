import { Optional } from 'sequelize';
import { Content } from '../models/content'; // Import Content model

// Create a new content
export const createContent = async (req: Optional<any, any>[]): Promise<any> => {
  const insertContent = await Content.bulkCreate(req);
  return { insertContent };
};

// Get a single content by identifier
export const getContentById = async (id: number): Promise<any> => {
  const contentDetails = await Content.findOne({
    where: { id },
  });
  return contentDetails;
};

// Get a single content by identifier
export const getAllContent = async (req: any): Promise<any> => {
  const contentDetails = await Content.findAll({
    where: req,
  });
  return contentDetails;
};
