package dev.lambdacraft.watchtower.mixins;

import dev.lambdacraft.watchtower.beans.Placement;
import dev.lambdacraft.watchtower.beans.Transaction;
import net.minecraft.block.Block;
import net.minecraft.block.BlockState;
import net.minecraft.block.AbstractBlock;
import net.minecraft.block.ChestBlock;
import net.minecraft.block.entity.BlockEntity;
import net.minecraft.block.entity.LockableContainerBlockEntity;
import net.minecraft.entity.player.PlayerEntity;
import net.minecraft.item.Items;
import net.minecraft.util.ActionResult;
import net.minecraft.util.Hand;
import net.minecraft.util.Identifier;
import net.minecraft.util.hit.BlockHitResult;
import net.minecraft.util.math.BlockPos;
import net.minecraft.world.World;

import java.util.Optional;
import java.util.UUID;

import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

import dev.lambdacraft.watchtower.Chat;
import dev.lambdacraft.watchtower.Commands;
import dev.lambdacraft.watchtower.DatabaseManager;
import dev.lambdacraft.watchtower.IChestBlockUUID;
import dev.lambdacraft.watchtower.IWatchTowerId;

@Mixin(AbstractBlock.AbstractBlockState.class)
public class BlockInteractionMixin {
  private static boolean isHoldingSelectorTool(PlayerEntity player) {
      return !player.getMainHandStack().isEmpty() && player.getMainHandStack().getItem() == Items.DIAMOND_SWORD;
  }

  private long lastClick = 0;

  /**
   * Check player interaction with blocks and item held in hand for logging query
   * tool
   */
  @Inject(method = "onUse",at = @At(value = "HEAD"),cancellable = true)
  private void tryInspectBlock(
    World world, PlayerEntity player, Hand hand, BlockHitResult hit,
    CallbackInfoReturnable<ActionResult> ret
  ) {
    if (lastClick + 1000 > System.currentTimeMillis()) {
      return;
    }
    lastClick = System.currentTimeMillis();

    Block block = ((BlockState)(Object)this).getBlock();
    BlockPos pos = hit.getBlockPos();
    BlockState state = world.getBlockState(pos);
    if (
      !isHoldingSelectorTool(player) ||
      !world.getServer().getPlayerManager().isOperator(player.getGameProfile()) ||
      !Commands.hasToolEnabled(player)
    ) {
      ret.setReturnValue(block.onUse(state, world, pos, player, hand, hit));
      return;
    }

    Block targetBlock = state.getBlock();
    BlockEntity be = world.getBlockEntity(pos);
    DatabaseManager dm = DatabaseManager.getSingleton();
    Identifier dimension = world.getRegistryKey().getValue();

    if (be instanceof LockableContainerBlockEntity) {
      Optional<UUID> opt;
      if (targetBlock instanceof ChestBlock) {
        opt = ((IChestBlockUUID)targetBlock).getWatchTowerIdAt(state, world, pos);
      } else {
        opt = Optional.of(((IWatchTowerId)be).getWatchTowerId());
      }
      opt.ifPresent(uuid -> {
        dm.getTransactionsFromUUID(uuid, 10).stream()
          .map(Transaction::getText)
          .reduce((t1, t2) -> Chat.concat("\n", t1, t2))
          .ifPresent(msg -> {
            Chat.send(player, Chat.concat("\n", Chat.text("Transaction history"), msg));
          });
      });
    }

    dm.getPlacementsAt(dimension, pos, 10).stream()
      .map(Placement::getText)
      .reduce((p1, p2) -> Chat.concat("\n", p1, p2))
      .ifPresent(msg -> {
        Chat.send(player, Chat.concat("\n", Chat.text("Placement history"), msg));
      });

    ret.setReturnValue(ActionResult.SUCCESS);
  }
}