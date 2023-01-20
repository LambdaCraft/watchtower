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
import net.minecraft.block.entity.BlockEntity;
import net.minecraft.block.entity.BlockEntityType;
import net.minecraft.block.entity.LockableContainerBlockEntity;

/** 
 * Log most container entity access events which have LockableContainerBlockEntity
 * as a super class.
 */
@Mixin(LockableContainerBlockEntity.class)
public abstract class LockableContainerBlockEntityMixin extends BlockEntity implements IWatchTowerId {
  public LockableContainerBlockEntityMixin(BlockEntityType<?> type, BlockPos pos, BlockState state) {
    super(type, pos, state);
  }

  private UUID watchtowerid = UUID.randomUUID();

  @Inject(at = @At("TAIL"), method = "readNbt")
  public void readNbt(NbtCompound nbt, CallbackInfo ci) {
    if (nbt.containsUuid(ItemUtils.NBT_TAG_KEY)) {
      this.watchtowerid = nbt.getUuid(ItemUtils.NBT_TAG_KEY);
      // System.out.println("HAS UUID " + this.watchtowerid);
    } else {
      this.watchtowerid = UUID.randomUUID();
      // System.out.println("ASSIGNED UUID " + this.watchtowerid);
    }
  }

  @Inject(at = @At("TAIL"), method = "writeNbt")
  public void writeNbt(NbtCompound nbt, CallbackInfo ci) {
    nbt.putUuid(ItemUtils.NBT_TAG_KEY, this.watchtowerid);
  }

  public UUID getWatchTowerId() {
    return watchtowerid;
  }
}