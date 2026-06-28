// Minimal ambient types for troika-three-text (the package ships no .d.ts, and
// there is no @types/troika-three-text on DefinitelyTyped). Only the surface
// used by GraphVisualization is declared. Written against troika-three-text
// 0.52.4; revisit if that dependency is upgraded.
declare module "troika-three-text" {
  import { Mesh, Material, Color } from "three";

  export class Text extends Mesh {
    text: string;
    fontSize: number;
    color: number | string | Color;
    anchorX: number | string;
    anchorY: number | string;
    font: string | null;
    outlineWidth: number | string;
    material: Material;
    sync(callback?: () => void): void;
    dispose(): void;
  }

  export function preloadFont(
    options: { font?: string; characters?: string | string[] },
    callback: () => void,
  ): void;
}
