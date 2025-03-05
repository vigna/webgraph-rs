import numpy as np
import matplotlib.pyplot as plt

def create_centered_barplot(data_dict, figsize=(16, 9)):
    vals = data_dict["default"]
    #if len(vals) > 10:
    #    low = np.percentile(vals, 10)
    #    high = np.percentile(vals, 90)
    #    vals = [x for x in vals if x > low and x < high]
    base = np.median(vals)
    
    for vals in data_dict.values():
        #if len(vals) > 10:
        #    low = np.percentile(vals, 10)
        #    high = np.percentile(vals, 90)
        #    vals[:] = [x for x in vals if x > low and x < high]
        vals[:] = [(base - x)/base for x in vals]
        
    # Calculate statistics  
    triples = [(key, np.median(values), np.std(values)) for key, values in data_dict.items()]
    triples.sort(key=lambda x: x[1])
    names, medians, stds = zip(*triples)

    # Create figure
    fig, ax = plt.subplots(figsize=figsize)
    
    # Y positions for the bars
    y_pos = np.arange(len(names))
    
    # Add individual points
    for i, name in enumerate(names):
        values = data_dict[name]
        ax.scatter(
            values, 
            [i] * len(values), 
            alpha=0.1, 
            color='navy',
            zorder=3, 
            s=10,
        )
        box = ax.boxplot(
            values, 
            vert=False, 
            positions=[i], 
            widths=0.8,
            patch_artist=True,
            boxprops=dict(facecolor='white', color='navy'),
            whiskerprops=dict(color='navy'),
            capprops=dict(color='navy'),
            medianprops=dict(color='navy'),
            flierprops=dict(markersize=0),
            zorder=2,
        )
        
        # Add the label directly on the left side of each box
        # Get the leftmost point of the box (usually the whisker end or flier)
        left_pos = min(*box["whiskers"][0].get_xdata(), *box["whiskers"][1].get_xdata())
        # Add some padding to position the text
        text_pos = left_pos - 0.01
        
        # Place the text label
        ax.text(
            text_pos, 
            i, 
            name, 
            verticalalignment='center',
            horizontalalignment='right',
            fontsize=10,
            fontweight='bold',
            color='black'
        )
    
    # Customize the plot
    ax.axvline(x=0, color='gray', linestyle='-', alpha=0.3, zorder=1)
    
    # Remove y-axis ticks and labels since we're adding our own labels
    ax.set_yticks([])
    ax.set_yticklabels([])
    
    # Format x-axis labels as percentages
    ax.set_xticklabels([f"{100*x:.1f}%" for x in ax.get_xticks()])
    
    # Add grid for better readability
    ax.grid(True, axis='x', alpha=0.3)
    
    # Adjust layout and margins to make room for the labels
    plt.tight_layout()
    # Add more left margin to accommodate the labels
    plt.subplots_adjust(left=0.2)
    
    return fig, ax

BLACKLIST = {
    "-Copt-level=0"
}

# Example usage:
if __name__ == "__main__":
    runtime_data = {}
    build_time_data = {}
    
    with open("results.csv") as f:
        txt = f.read()
    for line in txt.split("\n")[1:]:
        if not line:
            continue
        try:
            flags, build, runtime = line.split("\t")
            flags = flags.strip('"').strip()

            if flags in BLACKLIST:
                continue
            
            runtime_data.setdefault(flags, []).append(float(runtime))
            build_time_data.setdefault(flags, []).append(float(build))
        except ValueError:
            print(f"Error processing line: {line}")
    
    fig, ax = create_centered_barplot(runtime_data)
    
    # Optional: Customize further
    ax.set_xlabel('Run time Improvement (%)')
    ax.set_title('Compiler Flag Performance Impact')
    fig.savefig("runtime.png")
    plt.show()
    
    fig, ax = create_centered_barplot(build_time_data)
    
    # Optional: Customize further
    ax.set_xlabel('Build-time Improvement (%)')
    ax.set_title('Compiler Flag Performance Impact')
    fig.savefig("buildtime.png")
    plt.show()