package dev.lambdacraft.watchtower.mixins;

import java.util.UUID;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

import java.util.Optional;

import net.minecraft.inventory.DoubleInventory;
import net.minecraft.inventory.Inventory;
import net.minecraft.item.ItemStack;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

import dev.lambdacraft.watchtower.DatabaseManager;
import dev.lambdacraft.watchtower.IChestBlockUUID;
import dev.lambdacraft.watchtower.IWatchTowerId;
import dev.lambdacraft.watchtower.ItemUtils;
import net.minecraft.block.AbstractChestBlock;
import net.minecraft.block.BlockState;
import net.minecraft.block.ChestBlock;
import net.minecraft.block.DoubleBlockProperties;
import net.minecraft.block.entity.BlockEntityType;
import net.minecraft.block.entity.ChestBlockEntity;
import net.minecraft.entity.player.PlayerEntity;
import net.minecraft.util.ActionResult;
import net.minecraft.util.Hand;
import net.minecraft.util.Identifier;
import net.minecraft.util.hit.BlockHitResult;
import net.minecraft.util.math.BlockPos;
import net.minecraft.util.registry.Registry;
import net.minecraft.world.WorldAccess;
import net.minecraft.world.World;

@Mixin(ChestBlock.class)
public abstract class ChestBlockMixin extends AbstractChestBlock<ChestBlockEntity> implements IChestBlockUUID {
    protected ChestBlockMixin(final Settings settings,
                              final Supplier<BlockEntityType<? extends ChestBlockEntity>> entityTypeSupplier) {
    super(settings, entityTypeSupplier);
  }

  private static final DoubleBlockProperties.PropertyRetriever<ChestBlockEntity, Optional<Inventory>> INVENTORY_RETRIEVER = new DoubleBlockProperties.PropertyRetriever<ChestBlockEntity, Optional<Inventory>>() {
      public Optional<Inventory> getFromBoth(ChestBlockEntity chestBlockEntity, ChestBlockEntity chestBlockEntity2) {
          return Optional.of(new DoubleInventory(chestBlockEntity, chestBlockEntity2));
      }

      public Optional<Inventory> getFrom(ChestBlockEntity chestBlockEntity) {
          return Optional.of(chestBlockEntity);
      }

      public Optional<Inventory> getFallback() {
          return Optional.empty();
      }
  };

  /** Complicated double chest ID retrieving logic */
  private final DoubleBlockProperties.PropertyRetriever<ChestBlockEntity, Optional<UUID>> UUID_RETRIEVER = 
    new DoubleBlockProperties.PropertyRetriever<ChestBlockEntity, Optional<UUID>>() {
      public Optional<UUID> getFromBoth(final ChestBlockEntity left, final ChestBlockEntity right) {
        final IWatchTowerId chestBE = (IWatchTowerId) left;
        return Optional.of(chestBE.getWatchTowerId());
      }

      public Optional<UUID> getFrom(final ChestBlockEntity chestBlockEntity) {
        final IWatchTowerId chestBE = (IWatchTowerId) chestBlockEntity;
        final UUID uuid = chestBE.getWatchTowerId();

        return Optional.of(uuid);
      }

      public Optional<UUID> getFallback() {
        return Optional.empty();
      }
  }; 
  
  public DoubleBlockProperties.PropertySource<? extends ChestBlockEntity> getSource(BlockState state, World world, BlockPos pos, boolean ignoreBlocked) {
    BiPredicate<WorldAccess, BlockPos> biPredicate2;
    if (ignoreBlocked) {
       biPredicate2 = (worldAccess, blockPos) -> false;
    } else {
       biPredicate2 = ChestBlock::isChestBlocked;
    }

    ChestBlock chestblock = (ChestBlock)(Object)this;

    return DoubleBlockProperties.toPropertySource((BlockEntityType)this.entityTypeRetriever.get(), ChestBlock::getDoubleBlockType, ChestBlock::getFacing, chestblock.FACING, state, world, pos, biPredicate2);
 }

  public Optional<UUID> getWatchTowerIdAt(final BlockState state, final World world, final BlockPos pos) {
    return this.getSource(state, world, pos, false).apply(UUID_RETRIEVER);
  }

  /** Log chest opening in container table hook */
  @Inject(at = @At(value = "INVOKE", target = "Lnet/minecraft/entity/player/PlayerEntity;incrementStat(Lnet/minecraft/stat/Stat;)V"), method = "onUse")
  public void onUse(final BlockState state, final World world, final BlockPos pos, 
    final PlayerEntity player, final Hand hand, final BlockHitResult hit, final CallbackInfoReturnable<ActionResult> ret) {
      getWatchTowerIdAt(state, world, pos).ifPresent(chestWId -> {
        //TODO difference between player and currentScreenHandler??
        ((IWatchTowerId)player.currentScreenHandler).setWatchTowerId(chestWId);
        Identifier dimension = world.getRegistryKey().getValue();
  
        DatabaseManager.getSingleton().queueOp(new DatabaseManager.ContainerUpdate(
          chestWId, Registry.BLOCK.getId(this), pos, player.getUuid(), DatabaseManager.getTime(), dimension
        ));

        Inventory beforeInv = getBlockEntitySource(state, world, pos, true).apply(INVENTORY_RETRIEVER).orElse(null);
        ItemStack[] before = new ItemStack[beforeInv.size()];
        for (int i = 0; i < beforeInv.size(); i++) {
            before[i] = beforeInv.getStack(i).copy();
        }

        ItemUtils.registerContentListener(player.currentScreenHandler, before);
      });
  }
}