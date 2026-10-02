const serializeGeneralMessage = ({ message, apiUserId }) => ({
  id: message.id,
  content: message.content || '',
  senderId: message.senderId || null,
  senderName: message.senderName || null,
  receiverId: message.receiverId || null,
  isMine: message.senderId === apiUserId,
  purchaseId: message.purchaseId || null,
  customProjectRequestId: message.customProjectRequestId || null,
  projectTitle: message.projectTitle || null,
  createdAt: message.createdAt || null,
  read: Boolean(message.read)
});

const serializePurchaseMessage = ({ message, apiUserId, projectTitle }) => ({
  id: message.id,
  content: message.content || '',
  senderId: message.senderId || null,
  senderName: message.senderName || null,
  receiverId: message.receiverId || null,
  isMine: message.senderId === apiUserId,
  purchaseId: message.purchaseId || null,
  projectTitle: message.projectTitle || projectTitle,
  createdAt: message.createdAt || null,
  read: Boolean(message.read)
});

const registerGeneralMessageApiRoutes = ({ app, requireApiUserAuth, db, uuidv4 }) => {
  app.get('/api/messages', requireApiUserAuth, (req, res) => {
    const messages = db.messages()
      .filter((message) => message && (
        (message.senderId === req.apiUser.id && message.receiverId === 'admin') ||
        (message.senderId === 'admin' && message.receiverId === req.apiUser.id)
      ))
      .sort((a, b) => new Date(a.createdAt || 0) - new Date(b.createdAt || 0))
      .map((message) => serializeGeneralMessage({ message, apiUserId: req.apiUser.id }));

    return res.json({ messages });
  });

  app.post('/api/messages', requireApiUserAuth, (req, res) => {
    const content = String(req.body.content || '').trim();
    if (!content) return res.status(400).json({ error: 'محتوى الرسالة مطلوب' });

    const users = db.users();
    const currentUser = users.find((item) => item && item.id === req.apiUser.id && item.role === 'user');
    if (!currentUser) return res.status(404).json({ error: 'User not found' });

    const message = {
      id: uuidv4(),
      senderId: currentUser.id,
      senderName: currentUser.name,
      receiverId: 'admin',
      content,
      read: false,
      createdAt: new Date().toISOString()
    };

    const messages = db.messages();
    messages.push(message);
    db.saveMessages(messages);

    return res.status(201).json({
      message: {
        id: message.id,
        content: message.content,
        senderId: message.senderId,
        senderName: message.senderName,
        receiverId: message.receiverId,
        isMine: true,
        createdAt: message.createdAt,
        read: false
      }
    });
  });
};

const registerPurchaseMessageApiRoutes = ({
  app,
  requireApiUserAuth,
  db,
  uuidv4,
  getPurchaseChatContextForUser
}) => {
  app.get('/api/purchases/:id/messages', requireApiUserAuth, (req, res) => {
    const purchaseChat = getPurchaseChatContextForUser(req.params.id, req.apiUser.id);
    if (!purchaseChat) return res.status(404).json({ error: 'Purchase not found' });

    const messages = db.messages()
      .filter((message) => (
        message &&
        message.purchaseId === purchaseChat.purchaseId && (
          (message.senderId === req.apiUser.id && message.receiverId === 'admin') ||
          (message.senderId === 'admin' && message.receiverId === req.apiUser.id)
        )
      ))
      .sort((a, b) => new Date(a.createdAt || 0) - new Date(b.createdAt || 0))
      .map((message) => serializePurchaseMessage({
        message,
        apiUserId: req.apiUser.id,
        projectTitle: purchaseChat.projectTitle
      }));

    const allMessages = db.messages();
    let changed = false;
    allMessages.forEach((message) => {
      if (
        message &&
        message.purchaseId === purchaseChat.purchaseId &&
        message.senderId === 'admin' &&
        message.receiverId === req.apiUser.id &&
        !message.read
      ) {
        message.read = true;
        changed = true;
      }
    });
    if (changed) db.saveMessages(allMessages);

    return res.json({ purchase: purchaseChat, messages });
  });

  app.post('/api/purchases/:id/messages', requireApiUserAuth, (req, res) => {
    const purchaseChat = getPurchaseChatContextForUser(req.params.id, req.apiUser.id);
    if (!purchaseChat) return res.status(404).json({ error: 'Purchase not found' });

    const content = String(req.body.content || '').trim();
    if (!content) return res.status(400).json({ error: 'محتوى الرسالة مطلوب' });

    const messages = db.messages();
    const newMessage = {
      id: uuidv4(),
      senderId: req.apiUser.id,
      senderName: req.apiUser.name,
      receiverId: 'admin',
      content,
      read: false,
      purchaseId: purchaseChat.purchaseId,
      orderId: purchaseChat.orderId,
      projectTitle: purchaseChat.projectTitle,
      createdAt: new Date().toISOString()
    };
    messages.push(newMessage);
    db.saveMessages(messages);

    return res.status(201).json({
      message: {
        id: newMessage.id,
        content: newMessage.content,
        senderId: newMessage.senderId,
        senderName: newMessage.senderName,
        receiverId: newMessage.receiverId,
        isMine: true,
        purchaseId: newMessage.purchaseId,
        projectTitle: newMessage.projectTitle,
        createdAt: newMessage.createdAt,
        read: false
      }
    });
  });
};

module.exports = {
  registerGeneralMessageApiRoutes,
  registerPurchaseMessageApiRoutes
};
