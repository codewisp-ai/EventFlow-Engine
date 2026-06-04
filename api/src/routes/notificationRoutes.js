import { Router } from 'express';
import { ingestNotification } from '../controllers/notificationController.js';

const router = Router();

router.post('/trigger', ingestNotification);

export default router;