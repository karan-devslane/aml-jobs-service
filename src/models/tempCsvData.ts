import { DataTypes } from 'sequelize';
import { AppDataSource } from '../config';

export const tempCSVData = AppDataSource.define(
  'temp_csv',
  {
    id: {
      type: DataTypes.INTEGER,
      primaryKey: true,
      autoIncrement: true,
    },
    data: {
      type: DataTypes.JSONB,
      allowNull: false,
    },
    process_id: {
      type: DataTypes.UUID,
      allowNull: false,
    },
    status: {
      type: DataTypes.ENUM('progress', 'failed', 'success'),
      allowNull: false,
    },
    error_message: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    is_active: {
      type: DataTypes.BOOLEAN,
      allowNull: false,
    },
  },

  {
    tableName: 'temp_csv',
    timestamps: true,
    createdAt: 'created_at',
    updatedAt: 'updated_at',
  },
);
