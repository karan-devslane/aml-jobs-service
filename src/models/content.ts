import { DataTypes } from 'sequelize';
import { AppDataSource } from '../config';

export const Content = AppDataSource.define(
  'content',
  {
    id: {
      type: DataTypes.INTEGER,
      primaryKey: true,
      autoIncrement: true,
    },
    x_id: {
      type: DataTypes.STRING,
      allowNull: false,
      unique: true,
    },
    identifier: {
      type: DataTypes.STRING,
      allowNull: false,
      unique: true,
    },
    name: {
      type: DataTypes.JSONB,
      allowNull: false,
    },
    description: {
      type: DataTypes.JSONB,
      allowNull: true,
    },
    tenant: {
      type: DataTypes.JSONB,
      allowNull: true,
    },
    repository: {
      type: DataTypes.JSONB,
      allowNull: true,
    },
    taxonomy: {
      type: DataTypes.JSONB,
      allowNull: false,
    },
    sub_skills: {
      type: DataTypes.JSONB,
      allowNull: true,
    },
    gradient: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    status: {
      type: DataTypes.ENUM('draft', 'live'),
      allowNull: false,
    },
    media: {
      type: DataTypes.JSONB,
      allowNull: true,
    },
    created_by: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    updated_by: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    is_active: {
      type: DataTypes.BOOLEAN,
      allowNull: false,
      defaultValue: true,
    },
  },
  {
    tableName: 'content',
    timestamps: true,
    createdAt: 'created_at',
    updatedAt: 'updated_at',
  },
);
