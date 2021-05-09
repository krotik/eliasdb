/**
 * GameWorld object send from the backend.
 */
export interface GameWorld {
    backdrop: string | null; // Backdrop image for game world

    // Game world dimensions

    screenWidth: number;
    screenHeight: number;

    // HTML elment dimensions (scaled if different from game world dimensions)

    screenElementWidth: number;
    screenElementHeight: number;
}
