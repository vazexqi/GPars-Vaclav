package org.gparallelizer.actors.pooledActors

import org.gparallelizer.actors.AbstractActorGroup

/**
 * Provides a common super class fo pooled actor's groups.
 *
 * @author Vaclav Pech
 * Date: May 8, 2009
 */
public abstract class AbstractPooledActorGroup extends AbstractActorGroup {

    def AbstractPooledActorGroup() { }

    def AbstractPooledActorGroup(final boolean usedForkJoin) { super(usedForkJoin); }

    /**
     * Creates a new instance of PooledActor, using the passed-in closure as the body of the actor's act() method.
     * The created actor will belong to the pooled actor group.
     * @param handler The body of the newly created actor's act method.
     * @return A newly created instance of the AbstractPooledActor class
     */
    public final AbstractPooledActor actor(Closure handler) {
        final AbstractPooledActor actor = [act: handler] as AbstractPooledActor
        handler.delegate = actor
        actor.actorGroup = this
        return actor
    }
}