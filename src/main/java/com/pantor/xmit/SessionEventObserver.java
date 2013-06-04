// Copyright (c), Pantor Engineering AB, 2013-
// All rights reserved

package com.pantor.xmit;

import com.pantor.xmit.Session;

interface SessionEventObserver
{
   public void onEstablished (Session s);
   public void onRejected (String reason);
}
