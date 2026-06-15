import React from 'react';
import NewChatSidebar from '../components/newchat/NewChatSidebar';
import ChatMessages from '../components/newchat/ChatMessages';
import ChatInput from '../components/newchat/ChatInput';

const NewChatPage: React.FC = () => (
  <div className="flex h-full w-full" data-testid="new-chat-page">
    <div className="w-[280px] border-r bg-gray-50">
      <NewChatSidebar />
    </div>

    <div className="flex-1 flex flex-col">
      <div className="h-12 border-b flex items-center px-4">
        新对话
      </div>

      <div className="flex-1 overflow-y-auto p-4">
        <ChatMessages />
      </div>

      <div className="border-t p-3">
        <ChatInput />
      </div>
    </div>
  </div>
);

export default NewChatPage;
