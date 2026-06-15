import React from 'react';
import NewChatSecondarySidebar from '../components/newchat/NewChatSecondarySidebar';
import NewChatMain from '../components/newchat/NewChatMain';

const NewChatPage: React.FC = () => (
  <div className="flex h-full w-full overflow-hidden" data-testid="new-chat-page">
    <div className="w-[320px] flex-shrink-0 border-r border-slate-200 bg-[#f5f7fb]">
      <NewChatSecondarySidebar />
    </div>

    <div className="flex-1 flex flex-col bg-white">
      <NewChatMain />
    </div>
  </div>
);

export default NewChatPage;
