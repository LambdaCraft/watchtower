package dev.lambdacraft.watchtower.mixins;

import java.util.UUID;

import net.minecraft.nbt.NbtCompound;
import net.minecraft.util.math.BlockPos;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

import dev.lambdacraft.watchtower.IWatchTowerId;
import dev.lambdacraft.watchtower.ItemUtils;
import net.minecraft.block.BlockState;
import net.minecraft.block.entity.BlockEntityType;
import net.minecraft.block.entity.ShulkerBoxBlockEntity;
import net.minecraft.block.entity.LootableContainerBlockEntity;

/**
 * Assign UUID to shulker entities
 */
@Mixin(ShulkerBoxBlockEntity.class)
public abstract class ShulkerBoxBlockEntityMixin extends LootableContainerBlockEntity implements IWatchTowerId {
  protected ShulkerBoxBlockEntityMixin(BlockEntityType<?> blockEntityType, BlockPos blockPos, BlockState blockState) {
    super(blockEntityType, blockPos, blockState);
  }

  private UUID watchtowerid = UUID.randomUUID();

  @Inject(at = @At("TAIL"), method = "readNbt")
  public void fromTag(NbtCompound tag, CallbackInfo info) {
    if (tag.containsUuid(ItemUtils.NBT_TAG_KEY)) {
      this.watchtowerid = tag.getUuid(ItemUtils.NBT_TAG_KEY);
    } else {
      this.watchtowerid = UUID.randomUUID();
    }
  }

  @Inject(at = @At("TAIL"), method = "writeNbt")
  public void toTag(NbtCompound nbt, CallbackInfo ci) {
    nbt.putUuid(ItemUtils.NBT_TAG_KEY, this.watchtowerid);
  }

  public UUID getWatchTowerId() {
    return watchtowerid;
  }
}