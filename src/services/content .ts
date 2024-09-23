import { Optional } from 'sequelize';
import { Content } from '../models/content'; // Import Content model

// Create a new content
export const createContent = async (req: Optional<any, any>[]): Promise<any> => {
  const insertContent = await Content.bulkCreate(req);
  return { insertContent };
};
