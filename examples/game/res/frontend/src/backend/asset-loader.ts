export enum AssetType {
    ImageAsset = 'image',
    SoundAsset = 'sound'
}

export interface AssetDefinition {
    id: string;
    type: AssetType;
    file: string;
    image?: HTMLImageElement;
    audio?: HTMLAudioElement[];
}

export class AssetLoader {
    /**
     * Host this client is connected to.
     */
    protected host: string;

    /**
     * URL prefix for asset locations
     */
    protected assetURLPrefix: string;

    protected loadedAssets: Record<string, AssetDefinition> = {};

    public constructor(
        host: string = window.location.host,
        path: string = 'assets'
    ) {
        this.host = host;
        this.assetURLPrefix = `https://${host}/${path}/`;
    }

    public async preload(
        assets: AssetDefinition[],
        callback: (assets: Record<string, AssetDefinition>) => void
    ) {
        let loadCounter = 0;
        let checkFinishedLoading = () => {
            loadCounter++;
            console.log(`Asset ${loadCounter} of ${assets.length} loaded.`);
            if (loadCounter === assets.length) {
                callback(this.loadedAssets);
            }
        };

        for (let ass of assets) {
            if (ass.type === AssetType.ImageAsset) {
                var im = new Image();
                im.src = `${this.assetURLPrefix}${ass.file}`;
                im.onload = checkFinishedLoading;
                ass.image = im;
                this.loadedAssets[ass.id] = ass;
            } else if (ass.type === AssetType.SoundAsset) {
                var ad = new Audio();
                ad.src = `${this.assetURLPrefix}${ass.file}`;
                ad.oncanplaythrough = checkFinishedLoading;
                ass.audio = [ad];

                // Create multiple audio objects so audio effects can be overlapping

                for (let i = 0; i < 6; i++) {
                    var ad = new Audio();
                    ad.src = `${this.assetURLPrefix}${ass.file}`;
                    ass.audio.push(ad);
                }
                this.loadedAssets[ass.id] = ass;
            }
        }
    }
}
