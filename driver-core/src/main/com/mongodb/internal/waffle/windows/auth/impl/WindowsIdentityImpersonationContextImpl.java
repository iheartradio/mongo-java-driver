/**
 * Waffle (https://github.com/Waffle/waffle)
 *
 * Copyright (c) 2010-2016 Application Security, Inc.
 *
 * All rights reserved. This program and the accompanying materials are made available under the terms of the Eclipse
 * Public License v1.0 which accompanies this distribution, and is available at
 * https://www.eclipse.org/legal/epl-v10.html.
 *
 * Contributors: Application Security, Inc.
 */
package com.mongodb.internal.waffle.windows.auth.impl;

import com.sun.jna.platform.win32.Advapi32;
import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.Win32Exception;
import com.sun.jna.platform.win32.WinNT.HANDLE;

import com.mongodb.internal.waffle.windows.auth.IWindowsImpersonationContext;

/**
 * The Class WindowsIdentityImpersonationContextImpl.
 *
 * @author dblock[at]dblock[dot]org
 */
public class WindowsIdentityImpersonationContextImpl implements IWindowsImpersonationContext {

    /**
     * Impersonate a logged on user.
     * 
     * @param windowsIdentity
     *            Windows identity obtained via LogonUser.
     */
    public WindowsIdentityImpersonationContextImpl(final HANDLE windowsIdentity) {
        if (!Advapi32.INSTANCE.ImpersonateLoggedOnUser(windowsIdentity)) {
            throw new Win32Exception(Kernel32.INSTANCE.GetLastError());
        }
    }

    /*
     * (non-Javadoc)
     * @see waffle.windows.auth.IWindowsImpersonationContext#revertToSelf()
     */
    @Override
    public void revertToSelf() {
        Advapi32.INSTANCE.RevertToSelf();
    }
}
