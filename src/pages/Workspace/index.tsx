import React from 'react';
import DashboardPage from '../../components/DashboardPage';
import WorkspaceLayout from '../../components/workspace/WorkspaceLayout';
import type { PageType } from '../../components/layout';

export interface WorkspacePageProps {
  onNavigate: (page: PageType) => void;
}

const WorkspacePage: React.FC<WorkspacePageProps> = ({ onNavigate }) => (
  <WorkspaceLayout>
    <DashboardPage onNavigate={onNavigate} />
  </WorkspaceLayout>
);

export default WorkspacePage;
