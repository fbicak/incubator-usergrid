/*
 * Copyright 2014 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.usergrid.corepersistence;


import org.apache.usergrid.corepersistence.index.IndexLocationStrategyFactory;
import org.apache.usergrid.persistence.collection.EntityCollectionManager;
import org.apache.usergrid.persistence.collection.EntityCollectionManagerFactory;
import org.apache.usergrid.persistence.core.scope.ApplicationScope;
import org.apache.usergrid.persistence.graph.GraphManager;
import org.apache.usergrid.persistence.graph.GraphManagerFactory;
import org.apache.usergrid.persistence.index.EntityIndex;
import org.apache.usergrid.persistence.index.EntityIndex;
import org.apache.usergrid.persistence.index.EntityIndexFactory;
import org.apache.usergrid.persistence.index.IndexLocationStrategy;
import org.apache.usergrid.persistence.index.impl.IndexProducer;
import org.apache.usergrid.persistence.map.MapManager;
import org.apache.usergrid.persistence.map.MapManagerFactory;
import org.apache.usergrid.persistence.map.MapScope;

import com.google.inject.Inject;


/**
 * Cache for managing our other managers.  Now just a delegate.  Needs refactored away
 */
public class CpManagerCache implements ManagerCache {

    private final EntityCollectionManagerFactory ecmf;
    private final EntityIndexFactory eif;
    private final GraphManagerFactory gmf;
    private final MapManagerFactory mmf;
    private final IndexLocationStrategyFactory indexLocationStrategyFactory;
    private final IndexProducer indexProducer;

    // TODO: consider making these cache sizes and timeouts configurable


    @Inject
    public CpManagerCache( final EntityCollectionManagerFactory ecmf,
                           final EntityIndexFactory eif,
                           final GraphManagerFactory gmf,
                           final MapManagerFactory mmf,
                           final IndexLocationStrategyFactory indexLocationStrategyFactory,
                           final IndexProducer indexProducer
    ) {

        this.ecmf = ecmf;
        this.eif = eif;
        this.gmf = gmf;
        this.mmf = mmf;
        this.indexLocationStrategyFactory = indexLocationStrategyFactory;
        this.indexProducer = indexProducer;
    }


    @Override
    public EntityCollectionManager getEntityCollectionManager( ApplicationScope scope ) {
        //cache is now in the colletion manager level
        return ecmf.createCollectionManager( scope );
    }


    @Override
    public EntityIndex getEntityIndex( ApplicationScope applicationScope) {
        IndexLocationStrategy indexLocationStrategy
            = indexLocationStrategyFactory.getIndexLocationStrategy(applicationScope);
        return eif.createEntityIndex( indexLocationStrategy );
    }


    @Override
    public GraphManager getGraphManager( ApplicationScope appScope ) {
        return gmf.createEdgeManager( appScope );
    }


    @Override
    public MapManager getMapManager( MapScope mapScope ) {
        return mmf.createMapManager( mapScope );
    }

    @Override
    public IndexProducer getIndexProducer() {
        return indexProducer;
    }


    @Override
    public void invalidate() {
        ecmf.invalidate();
        eif.invalidate();
        gmf.invalidate();
        mmf.invalidate();
    }
}
