/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.usergrid.chop.api.store.subutai;


import org.safehaus.guicyfig.FigSingleton;
import org.safehaus.guicyfig.GuicyFig;
import org.safehaus.guicyfig.Key;


/**
 * Amazon configuration settings.
 */
@FigSingleton
public interface SubutaiFig extends GuicyFig {

    String SUBUTAI_PEER_SITE = "subutai.peer.site";

    @Key( SubutaiFig.SUBUTAI_PEER_SITE )
    String getSubutaiPeerSite();



    String SUBUTAI_AUTH_TOKEN = "subutai.auth.token";

    @Key( SubutaiFig.SUBUTAI_AUTH_TOKEN )
    String getSubutaiAuthToken();
}
