package com.celements.search.lucene.index.rebuild;

import static com.google.common.base.Preconditions.*;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import org.xwiki.component.annotation.ComponentRole;
import org.xwiki.model.reference.EntityReference;

import com.celements.model.util.References;
import com.google.common.collect.ImmutableList;
import com.xpn.xwiki.plugin.lucene.IndexUpdater;

@ComponentRole
public interface LuceneIndexRebuildService {

  void initialize(@NotNull IndexUpdater indexUpdater);

  @NotNull
  Optional<IndexRebuildFuture> getRunningRebuild();

  @NotNull
  Optional<IndexRebuildFuture> getQueuedRebuild(@Nullable EntityReference filterRef);

  @NotNull
  ImmutableList<IndexRebuildFuture> getQueuedRebuilds();

  @NotNull
  IndexRebuildFuture startIndexRebuild(@NotNull EntityReference filterRef);

  public class IndexRebuildFuture extends CompletableFuture<Long> {

    private final EntityReference ref;

    public IndexRebuildFuture(EntityReference ref) {
      this.ref = checkNotNull(ref);
    }

    @NotNull
    public EntityReference getReference() {
      return References.cloneRef(ref);
    }

    @Override
    public String toString() {
      return "[" + super.toString() + ", ref=" + ref + "]";
    }
  }

}
