export enum AnimationStyle {
    Forward = 'forward',
    ForwardAndBackward = 'forwardandbackward'
}

export class AnimationController {
    private framesImage: HTMLImageElement;
    private frameWidth: number;
    private frameHeight: number;
    private totalFrames: number;

    private style: AnimationStyle;
    private direction: boolean = true;

    private sequences: number;
    private tickTime: number;
    private callback: () => void;

    private currentFrame: number = -1;
    private currentFrameStartTime: number = 0;

    public constructor(
        framesImage: HTMLImageElement,
        framesWidth: number,
        framesHeight: number,
        style: AnimationStyle = AnimationStyle.Forward,
        sequences: number = -1,
        tickTime: number = 100,
        callback: () => void = () => {}
    ) {
        this.framesImage = framesImage;
        this.frameWidth = framesWidth;
        this.frameHeight = framesHeight;
        this.style = style;
        this.sequences = sequences;
        this.tickTime = tickTime;
        this.callback = callback;

        this.totalFrames = Math.floor(framesImage.width / this.frameWidth);
    }

    /**
     * Produce the current animation frame.
     */
    public tick(
        ctx: CanvasRenderingContext2D,
        dx: number,
        dy: number,
        dWidth: number,
        dHeight: number
    ): void {
        if (this.sequences == 0) {
            return;
        }

        let t = Date.now();
        if (t - this.currentFrameStartTime > this.tickTime) {
            this.currentFrameStartTime = t;

            // Time for the next frame

            if (this.direction) {
                this.currentFrame++;
            } else {
                this.currentFrame--;
            }

            if (
                this.currentFrame >= this.totalFrames - 1 ||
                this.currentFrame <= -1
            ) {
                if (this.currentFrame >= this.totalFrames - 1) {
                    if (this.style === AnimationStyle.ForwardAndBackward) {
                        this.direction = !this.direction;
                    } else {
                        this.currentFrame = 0;
                    }
                } else if (this.currentFrame < 0) {
                    if (this.style === AnimationStyle.ForwardAndBackward) {
                        this.direction = !this.direction;
                        this.currentFrame = 0;
                    } else {
                        this.currentFrame = this.totalFrames - 1;
                    }
                }

                if (this.sequences > 0) {
                    this.sequences--;

                    if (this.sequences == 0) {
                        this.callback();
                        return;
                    }
                }
            }
        }

        ctx.drawImage(
            this.framesImage,
            this.currentFrame * this.frameWidth,
            0,
            this.frameWidth,
            this.frameHeight,
            dx,
            dy,
            dWidth,
            dHeight
        );
    }
}

// Play a sound from a list of sound objects
//
export function playOneSound(sound: HTMLAudioElement[]) {
    for (var i = 0; i < sound.length; i++) {
        if (sound[i].paused) {
            sound[i].play();
            break;
        }
    }
}

// Play a looping sound
//
export async function playLoop(sound: HTMLAudioElement[]) {
    try {
        sound[0].loop = true
        await sound[0].play();
        console.log("Background!")
    } catch {
        setTimeout(() => {
            playLoop(sound)
        }, 100)
    }
}
