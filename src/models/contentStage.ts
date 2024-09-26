import { DataTypes } from 'sequelize';
import { AppDataSource } from '../config';

export const ContentStage = AppDataSource.define(
  'content_stage',
  {
    id: {
      type: DataTypes.INTEGER,
      primaryKey: true,
      autoIncrement: true,
    },
    process_id: {
      type: DataTypes.UUID,
      allowNull: false,
    },
    content_id: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    title: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    description: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    repository_name: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    board: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    class: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    L1_skill: {
      type: DataTypes.ENUM('Addition', 'Subtraction', 'Multiplication', 'Division'),
      allowNull: false,
    },
    L2_skill: {
      type: DataTypes.ARRAY(DataTypes.STRING),
      allowNull: false,
    },
    L3_skill: {
      type: DataTypes.ARRAY(DataTypes.STRING),
      allowNull: true,
    },
    sub_skills: {
      type: DataTypes.ARRAY(DataTypes.STRING),
      allowNull: true,
    },
    gradient: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    media_files: {
      type: DataTypes.JSONB,
      allowNull: false,
    },
    status: {
      type: DataTypes.ENUM('progress', 'errored', 'success'),
      allowNull: true,
    },
    error_info: {
      type: DataTypes.JSON,
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
  },
  {
    tableName: 'content_stage',
    timestamps: true,
    createdAt: 'created_at',
    updatedAt: 'updated_at',
  },
);
